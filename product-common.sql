SELECT products.is_bundle, products.name, products.seller_sku, categories.name, brands.name, colors.name, products.model, products.part_number, products.barcode, units.name, products.type, products.objective, products.warranty_months, products.warranty_description, products.weight, products.length, products.width, products.height
FROM products
JOIN categories ON products.category_id = categories.id
JOIN brands ON products.brand_id = brands.id
JOIN colors ON products.color_id = colors.id
JOIN units ON products.unit_id = units.id
LIMIT 1;
