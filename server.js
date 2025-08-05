const express = require('express');
const multer = require('multer');
const Papa = require('papaparse');
const fs = require('fs');
const path = require('path');
const cors = require('cors'); // Add CORS support
require('dotenv').config();
const app = express();
const upload = multer({ dest: 'uploads/' });
const Airtable = require('airtable'); // Ensure 'airtable' package is installed
// Enable CORS for all routes
app.use(cors({
  origin: '*', // Allow all origins for development
  methods: ['GET', 'POST', 'PUT', 'DELETE'],
  allowedHeaders: ['Content-Type', 'Authorization']
}));

// Store processed data in memory
let masterData = {
  skuToMsku: new Map(),
  comboSkus: new Map(),      // Store combo definitions
  currentInventory: new Map(),
  originalInventory: new Map() // Backup of original inventory
};

// Load master data on startup
async function loadMasterFilesOnStartup() {
  try {
    const dataDir = path.join(__dirname, 'data');
    if (!fs.existsSync(dataDir)) {
      fs.mkdirSync(dataDir);
      console.log('Created data/ directory. Please place your master CSV files here:');
      console.log('  - data/master-skus.csv (your "WMS-04-02 - Msku With Skus.csv")');
      console.log('  - data/combos.csv (your combo SKUs file)');
      console.log('  - data/current-inventory.csv (your current inventory)');
      return;
    }

    // Load master SKU mapping file
    const masterSkuFile = path.join(dataDir, 'master-skus.csv');
    if (fs.existsSync(masterSkuFile)) {
      const masterFileContent = fs.readFileSync(masterSkuFile, 'utf8');
      await loadMasterData(masterFileContent);
      console.log('✅ Master SKU mappings loaded from data/master-skus.csv');
    } else {
      console.log('⚠️  No master-skus.csv found in data/ directory');
    }

    // Load combo file
    const comboFile = path.join(dataDir, 'combos.csv');
    if (fs.existsSync(comboFile)) {
      const comboContent = fs.readFileSync(comboFile, 'utf8');
      await loadComboData(comboContent);
      console.log('✅ Combo SKUs loaded from data/combos.csv');
    } else {
      console.log('📋 No combos.csv found (optional file)');
    }

    // Load current inventory file
    const inventoryFile = path.join(dataDir, 'current-inventory.csv');
    if (fs.existsSync(inventoryFile)) {
      const inventoryContent = fs.readFileSync(inventoryFile, 'utf8');
      await loadCurrentInventory(inventoryContent);
      console.log('✅ Current inventory loaded from data/current-inventory.csv');
    } else {
      console.log('⚠️  No current-inventory.csv found in data/ directory');
    }

  } catch (error) {
    console.error('Error loading master files:', error);
  }
}

// Parse CSV helper function
function parseCSV(csvContent) {
  return new Promise((resolve, reject) => {
    Papa.parse(csvContent, {
      header: true,
      skipEmptyLines: true,
      dynamicTyping: true,
      delimitersToGuess: [',', '\t', '|', ';'],
      complete: (results) => {
        console.log(`Parsed ${results.data.length} rows`);
        if (results.data.length > 0) {
          console.log('Sample headers:', Object.keys(results.data[0]));
        }
        resolve(results.data);
      },
      error: (error) => {
        reject(error);
      }
    });
  });
}

// Load master SKU mapping data
function loadMasterData(csvContent) {
  return parseCSV(csvContent).then(data => {
    console.log('Loading master SKU mappings...');
    
    data.forEach((row, index) => {
      // Handle different possible column names (case-insensitive)
      const sku = findColumnValue(row, ['sku', 'SKU', 'Sku']);
      const msku = findColumnValue(row, ['msku', 'MSKU', 'Msku']);
      const status = findColumnValue(row, ['status', 'Status', 'STATUS']);
      
      if (sku && msku && sku !== msku) {
        masterData.skuToMsku.set(sku.toString().trim(), msku.toString().trim());
      }
      
      // Debug: Log first few rows
      if (index < 3) {
        console.log(`Row ${index}:`, { sku, msku, status });
      }
    });
    
    console.log(`Loaded ${masterData.skuToMsku.size} SKU mappings`);
    return masterData;
  });
}

// Updated loadComboData function to handle your wide format
function loadComboData(csvContent) {
  return parseCSV(csvContent).then(data => {
    console.log('Loading combo SKU data...');
    
    data.forEach((row, index) => {
      // Handle your actual column names
      const comboSku = findColumnValue(row, ['Combo ', 'Combo', 'combo']);
      const status = findColumnValue(row, ['Status', 'status', 'STATUS']);
      
      // Initialize components array
      const components = [];
      
      // Process combos with status 'Combo' or 'Active' or no status
      if (comboSku && (!status || 
          status.toString().toLowerCase() === 'combo' || 
          status.toString().toLowerCase() === 'active')) {
        
        const combo = comboSku.toString().trim();
        
        // Check SKU1 through SKU14 columns
        for (let i = 1; i <= 14; i++) {
          const component = findColumnValue(row, [`SKU${i}`, `sku${i}`]);
          if (component && component.toString().trim() !== '') {
            components.push({
              msku: component.toString().trim(),
              quantity: 1 // Default quantity of 1 per component
            });
          }
        }
        
        if (components.length > 0) {
          masterData.comboSkus.set(combo, components);
          console.log(`✅ Loaded combo ${combo} with ${components.length} components:`, components.map(c => c.msku));
        }
      }
      
      // Debug: Log first few rows
      if (index < 3) {
        console.log(`Combo Row ${index}:`, { 
          comboSku, 
          status, 
          componentCount: components.length,
          components: components.length > 0 ? components.map(c => c.msku) : 'none'
        });
      }
    });
    
    console.log(`Loaded ${masterData.comboSkus.size} combo SKU definitions`);
    return masterData;
  });
}

// Updated loadCurrentInventory function to handle your format
function loadCurrentInventory(csvContent) {
  return parseCSV(csvContent).then(data => {
    console.log('Loading current inventory...');
    console.log('Available columns:', data.length > 0 ? Object.keys(data[0]) : 'No data');
    
    data.forEach((row, index) => {
      const msku = findColumnValue(row, ['msku', 'MSKU', 'Msku']);
      const productName = findColumnValue(row, ['Product Name', 'product_name', 'Product']);
      const openingStock = parseInt(findColumnValue(row, ['Opening Stock', 'opening_stock', 'Opening']) || 0);
      const bufferStock = parseInt(findColumnValue(row, ['Buffer Stock', 'buffer_stock', 'Buffer']) || 0);
      
      // Calculate total warehouse stock (sum all warehouse columns)
      const warehouseColumns = ['TLCQ', 'BLR7', 'BLR8', 'BOM5', 'BOM7', 'CCU1', 'CCX1', 'DEL4', 'DEL5', 'DEX3', 'PNQ2', 'PNQ3', 'SDED', 'SDEE', 'XHJ9'];
      let warehouseStock = 0;
      let warehouseBreakdown = {};
      
      warehouseColumns.forEach(col => {
        const stock = parseInt(findColumnValue(row, [col]) || 0);
        if (stock > 0) {
          warehouseStock += stock;
          warehouseBreakdown[col] = stock;
        }
      });
      
      // Use opening stock as primary, warehouse total as secondary
      let totalStock = openingStock;
      if (totalStock === 0 && warehouseStock > 0) {
        totalStock = warehouseStock;
      }
      
      if (msku && msku.toString().trim() !== '') {
        const msquKey = msku.toString().trim();
        const inventoryData = {
          msku: msquKey,
          productName: productName ? productName.toString().trim() : 'Unknown Product',
          panel: Object.keys(warehouseBreakdown).length > 0 ? 
                 `Warehouses: ${Object.keys(warehouseBreakdown).join(', ')}` : 
                 'Unknown',
          status: totalStock > 0 ? 'In Stock' : 'Out of Stock',
          currentStock: totalStock,
          originalStock: totalStock,
          openingStock: openingStock,
          bufferStock: bufferStock,
          warehouseStock: warehouseStock,
          warehouseBreakdown: warehouseBreakdown
        };
        
        masterData.currentInventory.set(msquKey, inventoryData);
        masterData.originalInventory.set(msquKey, { ...inventoryData });
      }
      
      // Debug: Log first few rows
      if (index < 3) {
        console.log(`Inventory Row ${index}:`, { 
          msku, 
          productName: productName ? productName.substring(0, 30) + '...' : null, 
          openingStock, 
          warehouseStock, 
          totalStock 
        });
      }
    });
    
    console.log(`Loaded ${masterData.currentInventory.size} inventory items`);
    return masterData;
  });
}

// Helper function to find column value with different naming conventions
function findColumnValue(row, possibleNames) {
  for (const name of possibleNames) {
    if (row[name] !== undefined && row[name] !== null && row[name] !== '') {
      return row[name];
    }
  }
  return null;
}

// Enhanced marketplace detection
function detectMarketplace(orderData) {
  if (!orderData || orderData.length === 0) return 'unknown';
  
  const firstRow = orderData[0];
  const columns = Object.keys(firstRow).map(k => k.toLowerCase());
  
  console.log('Available columns for marketplace detection:', columns);
  
  // Check for marketplace-specific columns
  if (columns.some(col => col.includes('msku'))) {
    return 'meesho';
  } else if (columns.some(col => col.includes('asin') || col.includes('amazon'))) {
    return 'amazon';
  } else if (columns.some(col => col.includes('flipkart') || col.includes('fsn'))) {
    return 'flipkart';
  } else if (columns.some(col => col.includes('sku'))) {
    // Generic marketplace with SKU
    return 'generic';
  }
  
  return 'unknown';
}

// Map SKU to MSKU based on marketplace
function mapSkuToMsku(sku, marketplace) {
  const cleanSku = sku ? sku.toString().trim() : '';
  
  if (marketplace === 'meesho') {
    // For Meesho, the SKU might already be MSKU
    return cleanSku;
  } else {
    // For Amazon/Flipkart/Generic, use mapping table
    if (masterData.skuToMsku.has(cleanSku)) {
      return masterData.skuToMsku.get(cleanSku);
    }
    return cleanSku; // Return as-is if no mapping found
  }
}

// Check if SKU is a combo and expand it
function expandComboSku(sku, marketplace) {
  const mappedMsku = mapSkuToMsku(sku, marketplace);
  
  // Check if this MSKU is a combo
  if (masterData.comboSkus.has(mappedMsku)) {
    return masterData.comboSkus.get(mappedMsku);
  }
  
  // Check if original SKU is a combo
  if (masterData.comboSkus.has(sku)) {
    return masterData.comboSkus.get(sku);
  }
  
  // Not a combo, return single item
  return [{ msku: mappedMsku, quantity: 1 }];
}

// Enhanced order information extraction
function extractOrderInfo(order, marketplace) {
  let sku, quantity, status, orderDate, customerLocation, productName;
  
  switch (marketplace) {
    case 'amazon':
      sku = findColumnValue(order, ['SKU', 'sku', 'ASIN', 'asin']);
      quantity = parseInt(findColumnValue(order, ['Quantity', 'quantity', 'Qty', 'qty']) || 1);
      status = findColumnValue(order, ['Order Status', 'order-status', 'Status', 'status']);
      orderDate = findColumnValue(order, ['Order Date', 'order-date', 'Purchase Date', 'Date']);
      customerLocation = findColumnValue(order, ['Ship City', 'ship-city', 'Customer State', 'Location']);
      productName = findColumnValue(order, ['Product Name', 'product-name', 'Title', 'Item']);
      break;
      
    case 'flipkart':
      sku = findColumnValue(order, ['SKU', 'sku', 'FSN', 'fsn']);
      quantity = parseInt(findColumnValue(order, ['Quantity', 'quantity', 'Qty', 'qty']) || 1);
      status = findColumnValue(order, ['Order Status', 'order-status', 'Status', 'status']);
      orderDate = findColumnValue(order, ['Order Date', 'order-date', 'Date']);
      customerLocation = findColumnValue(order, ['Customer State', 'customer-state', 'Location']);
      productName = findColumnValue(order, ['Product Name', 'product-name', 'Item']);
      break;
      
    case 'meesho':
      sku = findColumnValue(order, ['MSKU', 'msku', 'SKU', 'sku']);
      quantity = parseInt(findColumnValue(order, ['Quantity', 'quantity', 'Qty', 'qty']) || 1);
      status = findColumnValue(order, ['Status', 'status', 'Order Status']);
      orderDate = findColumnValue(order, ['Order Date', 'order-date', 'Date']);
      customerLocation = findColumnValue(order, ['Customer Location', 'customer-location', 'Location']);
      productName = findColumnValue(order, ['Product Name', 'product-name', 'Item']);
      break;
      
    case 'generic':
    default:
      // Generic extraction with more flexible column matching
      sku = findColumnValue(order, ['SKU', 'sku', 'MSKU', 'msku', 'Product Code', 'Item Code']);
      quantity = parseInt(findColumnValue(order, ['Quantity', 'quantity', 'Qty', 'qty', 'Count', 'Amount']) || 1);
      status = findColumnValue(order, ['Status', 'status', 'Order Status', 'Reason for Credit Entry', 'State']);
      orderDate = findColumnValue(order, ['Order Date', 'order-date', 'Date', 'Created Date']);
      customerLocation = findColumnValue(order, ['Customer State', 'Customer Location', 'Location', 'State', 'City']);
      productName = findColumnValue(order, ['Product Name', 'product-name', 'Title', 'Item Name', 'Description']);
  }
  
  return { sku, quantity, status, orderDate, customerLocation, productName };
}

// Enhanced order status checking
function shouldProcessOrder(status, marketplace) {
  if (!status) return false;
  
  const statusLower = status.toString().toLowerCase();
  
  // Common delivered/shipped statuses across marketplaces
  const validStatuses = [
    'delivered', 'shipped', 'ready_to_ship', 'dispatched', 
    'completed', 'fulfilled', 'out_for_delivery', 'success',
    'confirmed', 'processing', 'packed', 'in_transit'
  ];
  
  // Also check for orders that are not cancelled/returned
  const invalidStatuses = [
    'cancelled', 'returned', 'refunded', 'rejected', 'failed'
  ];
  
  const hasValidStatus = validStatuses.some(validStatus => statusLower.includes(validStatus));
  const hasInvalidStatus = invalidStatuses.some(invalidStatus => statusLower.includes(invalidStatus));
  
  return hasValidStatus && !hasInvalidStatus;
}

// Process order data
function processOrderData(orderData, marketplace) {
  const processedOrders = [];
  const msquQuantityMap = new Map();
  const unmappedSkus = new Set();
  
  console.log(`Processing ${orderData.length} orders from ${marketplace}`);
  
  orderData.forEach((order, index) => {
    const { sku, quantity, status, orderDate, customerLocation, productName } = extractOrderInfo(order, marketplace);
    
    // Debug: Log first few orders
    if (index < 3) {
      console.log(`Order ${index}:`, { sku, quantity, status, orderDate });
    }
    
    // Only process delivered/shipped orders
    if (shouldProcessOrder(status, marketplace)) {
      if (sku && quantity > 0) {
        const expandedItems = expandComboSku(sku, marketplace);
        
        expandedItems.forEach(item => {
          const msku = item.msku;
          const itemQuantity = item.quantity * quantity;
          
          if (msku && msku !== '') {
            // Add to MSKU quantity map
            const currentQty = msquQuantityMap.get(msku) || 0;
            msquQuantityMap.set(msku, currentQty + itemQuantity);
            
            processedOrders.push({
              marketplace: marketplace,
              originalSku: sku,
              mappedMsku: msku,
              quantity: itemQuantity,
              status: status,
              orderDate: orderDate,
              customerLocation: customerLocation,
              productName: productName,
              isComboComponent: expandedItems.length > 1
            });
          } else {
            unmappedSkus.add(sku);
          }
        });
      }
    } else {
      console.log(`Skipping order with status: ${status}`);
    }
  });
  
  console.log(`Processed ${processedOrders.length} valid orders`);
  
  return {
    processedOrders,
    msquQuantityMap,
    unmappedSkus: Array.from(unmappedSkus)
  };
}

// Update inventory by subtracting sold quantities
function updateInventoryWithSales(msquQuantityMap) {
  const inventoryUpdates = [];
  
  msquQuantityMap.forEach((soldQuantity, msku) => {
    const inventoryItem = masterData.currentInventory.get(msku);
    
    if (inventoryItem) {
      const newStock = Math.max(0, inventoryItem.currentStock - soldQuantity);
      const stockDifference = inventoryItem.currentStock - newStock;
      
      // Update the inventory
      inventoryItem.currentStock = newStock;
      masterData.currentInventory.set(msku, inventoryItem);
      
      inventoryUpdates.push({
        msku: msku,
        originalStock: inventoryItem.originalStock,
        soldQuantity: soldQuantity,
        newStock: newStock,
        stockReduced: stockDifference,
        panel: inventoryItem.panel,
        status: inventoryItem.status,
        isOutOfStock: newStock === 0
      });
    } else {
      // MSKU not found in inventory
      inventoryUpdates.push({
        msku: msku,
        originalStock: 0,
        soldQuantity: soldQuantity,
        newStock: 0,
        stockReduced: 0,
        panel: 'Not Found',
        status: 'Unknown',
        isOutOfStock: true,
        notInInventory: true
      });
    }
  });
  
  return inventoryUpdates.sort((a, b) => b.soldQuantity - a.soldQuantity);
}

// Reset inventory to original state
function resetInventory() {
  masterData.originalInventory.forEach((originalData, msku) => {
    masterData.currentInventory.set(msku, { ...originalData });
  });
}

// API Routes
app.use(express.json());

// Upload and process files
app.post('/process-orders', upload.fields([
  { name: 'masterFile', maxCount: 1 },
  { name: 'orderFiles', maxCount: 10 }
]), async (req, res) => {
  try {
    console.log('Processing uploaded files...');
    
    // Load master data if uploaded
    if (req.files.masterFile && req.files.masterFile[0]) {
      const masterFileContent = fs.readFileSync(req.files.masterFile[0].path, 'utf8');
      await loadMasterData(masterFileContent);
      console.log('Master data loaded from uploaded file');
    } else if (masterData.skuToMsku.size === 0) {
      return res.status(400).json({ 
        error: 'No master data available. Either upload masterFile or place master-skus.csv in data/ directory' 
      });
    }
    
    // Reset inventory to original state before processing
    resetInventory();
    
    // Process order files
    const allProcessedOrders = [];
    const combinedMsquMap = new Map();
    const allUnmappedSkus = new Set();
    const marketplaceSummary = {};
    
    if (req.files.orderFiles) {
      for (const file of req.files.orderFiles) {
        console.log(`\n--- Processing file: ${file.originalname} ---`);
        const orderFileContent = fs.readFileSync(file.path, 'utf8');
        const orderData = await parseCSV(orderFileContent);
        
        // Detect marketplace
        const marketplace = detectMarketplace(orderData);
        console.log(`Detected marketplace: ${marketplace} for file: ${file.originalname}`);
        
        const result = processOrderData(orderData, marketplace);
        allProcessedOrders.push(...result.processedOrders);
        
        // Combine MSKU quantities
        result.msquQuantityMap.forEach((qty, msku) => {
          const currentQty = combinedMsquMap.get(msku) || 0;
          combinedMsquMap.set(msku, currentQty + qty);
        });
        
        result.unmappedSkus.forEach(sku => allUnmappedSkus.add(sku));
        
        // Track marketplace summary
        marketplaceSummary[marketplace] = {
          ordersProcessed: result.processedOrders.length,
          uniqueMskus: result.msquQuantityMap.size,
          totalQuantity: Array.from(result.msquQuantityMap.values()).reduce((sum, qty) => sum + qty, 0),
          unmappedSkus: result.unmappedSkus.length
        };
      }
    }
    
    // Update inventory with sales
    const inventoryUpdates = updateInventoryWithSales(combinedMsquMap);
    
    // Generate summary
    const summary = {
      totalOrdersProcessed: allProcessedOrders.length,
      uniqueMskusAffected: combinedMsquMap.size,
      totalQuantitySold: Array.from(combinedMsquMap.values()).reduce((sum, qty) => sum + qty, 0),
      unmappedSkusCount: allUnmappedSkus.size,
      outOfStockItems: inventoryUpdates.filter(item => item.isOutOfStock).length,
      marketplaceSummary
    };
    
    // Clean up uploaded files
    if (req.files.masterFile) {
      fs.unlinkSync(req.files.masterFile[0].path);
    }
    if (req.files.orderFiles) {
      req.files.orderFiles.forEach(file => fs.unlinkSync(file.path));
    }
    
    res.json({
      success: true,
      summary,
      inventoryUpdates,
      processedOrders: allProcessedOrders.slice(0, 100), // Limit for response size
      unmappedSkus: Array.from(allUnmappedSkus),
      message: 'Orders processed successfully and inventory updated'
    });
    
  } catch (error) {
    console.error('Error processing files:', error);
    res.status(500).json({ 
      error: 'Failed to process files', 
      details: error.message 
    });
  }
});

// Reset inventory endpoint
app.post('/reset-inventory', (req, res) => {
  resetInventory();
  res.json({ 
    success: true, 
    message: 'Inventory reset to original state' 
  });
});

// Get current mappings
app.get('/mappings', (req, res) => {
  const mappings = Array.from(masterData.skuToMsku.entries()).map(([sku, msku]) => ({
    sku,
    msku
  }));
  
  res.json({
    totalMappings: mappings.length,
    mappings: mappings.slice(0, 100)
  });
});

// Get combo definitions
app.get('/combos', (req, res) => {
  const combos = Array.from(masterData.comboSkus.entries()).map(([comboSku, components]) => ({
    comboSku,
    components
  }));
  
  res.json({
    totalCombos: combos.length,
    combos: combos
  });
});

// Get inventory status
app.get('/inventory', (req, res) => {
  const inventory = Array.from(masterData.currentInventory.entries()).map(([msku, data]) => ({
    msku,
    ...data
  }));
  
  res.json({
    totalItems: inventory.length,
    inventory: inventory.slice(0, 100)
  });
});

// Get updated inventory (showing changes)
app.get('/inventory-changes', (req, res) => {
  const changes = [];
  
  masterData.currentInventory.forEach((currentData, msku) => {
    const originalData = masterData.originalInventory.get(msku);
    
    if (originalData && currentData.currentStock !== originalData.currentStock) {
      changes.push({
        msku: msku,
        originalStock: originalData.currentStock,
        currentStock: currentData.currentStock,
        difference: originalData.currentStock - currentData.currentStock,
        panel: currentData.panel,
        status: currentData.status
      });
    }
  });
  
  res.json({
    totalChanges: changes.length,
    changes: changes.sort((a, b) => b.difference - a.difference)
  });
});

// Health check
app.get('/health', (req, res) => {
  res.json({ 
    status: 'OK', 
    mappingsLoaded: masterData.skuToMsku.size,
    combosLoaded: masterData.comboSkus.size,
    inventoryItems: masterData.currentInventory.size
  });
});

app.get('/test-airtable', async (req, res) => {
    try {
        // Hardcoded Airtable API key and base ID
        const apiKey = 'patWE4usCxpkybDwn.2d02c7863cbf8498f1c52c977a441e3d47119b3aec068cb36889ed2d910c2da6';
        const baseId = 'app1WKvRLMqf35PJh';
        
        // Initialize Airtable with the hardcoded values
        const airtable = new Airtable({ apiKey: apiKey });
        const base = airtable.base(baseId);
        
        // Fetch a few records from the 'Products' table as a test
        const records = await base('Products').select({ maxRecords: 3 }).firstPage();
        
        // Send a success response
        res.json({
            success: true,
            message: 'Airtable connection successful!',
            recordCount: records.length,
            baseId: baseId
        });
    } catch (error) {
        // Handle any errors
        res.status(500).json({
            success: false,
            error: error.message
        });
    }
});
// Error handling middleware
app.use((error, req, res, next) => {
  console.error('Unhandled error:', error);
  res.status(500).json({ error: 'Internal server error' });
});

const PORT = process.env.PORT || 3000;

// Load master files on startup
loadMasterFilesOnStartup().then(() => {
  app.listen(PORT, () => {
    console.log(`WMS Backend server running on port ${PORT}`);
    console.log('CORS enabled for all origins');
    console.log('Available endpoints:');
    console.log('  POST /process-orders     - Upload order CSVs and process inventory');
    console.log('  POST /reset-inventory    - Reset inventory to original state');
    console.log('  GET  /mappings          - View SKU to MSKU mappings');
    console.log('  GET  /combos            - View combo SKU definitions');
    console.log('  GET  /inventory         - View current inventory status');
    console.log('  GET  /inventory-changes - View inventory changes after processing');
    console.log('  GET  /health            - Check server status');
  });
});

module.exports = app;