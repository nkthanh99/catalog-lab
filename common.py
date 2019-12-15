import pandas as pd
import pymysql.cursors

def get_product_id(filters=None):
    conn = pymysql.connect(host='14.225.2.95',
                           user='cuong-dev',
                           password='secret',
                           db='catalog_dev')
    try:
        with conn.cursor() as cur:
            sql = 'SELECT id FROM products'
            if filters:
                s = ' AND '.join([f'{key} = %s' for key, _ in filters.items()])
                sql = f'{sql} WHERE {s}'
                sql = sql + ' LIMIT 1000'
                cur.execute(sql, list(filters.values()))
            else:
                sql = sql + ' LIMIT 1000'
                cur.execute(sql)
            result = cur.fetchall()
    except Exception as err:
        print(str(err))
        return []
    else:
        ret = [item[0] for item in result]
        return ret
    finally:
        conn.close()
        
        
def load_products(list_id):
    sql = f'SELECT products.is_bundle as "Type", products.name as "Name", products.seller_sku as "Seller SKU", categories.name as "Category", brands.name as "Brand", colors.name as "Color", products.model as "Model", products.part_number as "Part number", products.barcode as "Barcode", units.name as "Unit", products.type as "Product type", products.objective as "Objective", products.warranty_months as "Warranty Period (month)", products.warranty_description as "Warranty Note", products.weight as "Weight", products.length as "Length", products.width as "Width", products.height as "Height", product_description.short_description as " Short description", product_description.description as "Description", product_description.display_name as "Display name", product_description.meta_keyword as "meta keyword", product_description.meta_description as "Meta Description", sale_categories.name as "Sale category", product_properties_movesrm.tax_in as "Tax in", product_properties_movesrm.tax_out as "Tax out" FROM products JOIN categories ON products.category_id = categories.id JOIN brands ON products.brand_id = brands.id JOIN colors ON products.color_id = colors.id JOIN units ON products.unit_id = units.id JOIN product_description ON products.id = product_description.product_id JOIN sale_category_product ON sale_category_product.product_id = products.id JOIN sale_categories ON sale_categories.id = sale_category_product.sale_category_id JOIN product_properties_movesrm ON products.sku = product_properties_movesrm.sku WHERE products.id IN ({",".join(["%s"]*len(list_id))})'
    conn = pymysql.connect(host='14.225.2.95', user='cuong-dev', password='secret', db='catalog_dev')
    try:
        with conn.cursor(cursor=pymysql.cursors.DictCursor) as cur:
            cur.execute(sql, list_id)
            result = cur.fetchall()
    except:
        return pd.DataFrame([])
    else:
        for product in result:
            product['Type'] = 'Bundle' if product['Type'] else 'Simple'
        return pd.DataFrame(result)
    finally:
        conn.close()
        
def get_products(n):
    conn = pymysql.connect(host='14.225.2.95',
                           user='cuong-dev',
                           password='secret',
                           db='catalog_dev')
    try:
        with conn.cursor(cursor=pymysql.cursors.DictCursor) as cur:
            if n is None:
                sql = 'SELECT * FROM products'
                cur.execute(sql)
            else:
                sql = 'SELECT * FROM products LIMIT %s'
                cur.execute(sql, n)
            result = cur.fetchall()
    except:
        return []
    else:
        return result
    finally:
        conn.close()

def break_arr(arr, chunk_size=200):
    chunks = []
    _s = 0
    if len(arr) < chunk_size:
        return [arr]
    while _s < len(arr):
        chunks.append(arr[_s:_s+chunk_size])
        if _s + chunk_size > len(arr):
            chunks.append(arr[_s+chunk_size:])
            break
        _s += chunk_size
    return chunks