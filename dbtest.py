import logging
import mysql.connector
from modules.runTimeSecrets import HOST, DB, PASS, USER

# logger
# ------------------------------------------------------------
def loggerInit(logFileName):
    logger = logging.getLogger(__name__)
    logger.setLevel(logging.DEBUG)
    formatter = logging.Formatter('%(asctime)s:%(name)s:%(message)s')
    file_handler = logging.FileHandler(f'logs/{logFileName}')
    file_handler.setLevel(logging.DEBUG)
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)
    stream_handler = logging.StreamHandler()
    stream_handler.setFormatter(formatter)
    stream_handler.setLevel(logging.DEBUG)
    logger.addHandler(stream_handler)
    return logger
logger = loggerInit(logFileName="DBtest.log")
# ---------------------------------------------------------------

# Add product if doesn't exists
def checkInsertProduct(vendor_id, brand_id, mpn):
    conn = None
    this = None
    try:
        print("vbjbv ")
        print("Trying to connect...")
        conn = mysql.connector.connect(host=HOST, database=DB, user=USER, password=PASS)
        print("Connected!")
        if conn.is_connected():
            this = conn.cursor()
            checkProductQuery = "SELECT product_id FROM Product WHERE brand_id = %s AND product_mpn = %s"
            this.execute(checkProductQuery, [brand_id,mpn])
            records = this.fetchone()
            # Change this section
            if records is None:  # If no record found
                # Insert new product
                print("yes")
                # if msrp != '':
                #     insertProductQuery = "INSERT INTO Product (brand_id,product_name,product_mpn,msrp,product_image) VALUES (%s,%s,%s,%s,%s)"
                #     this.execute(insertProductQuery, (brand_id,name,mpn,msrp,image))
                # else:
                #     insertProductQuery = "INSERT INTO Product (brand_id,product_name,product_mpn,product_image) VALUES (%s,%s,%s,%s)"
                #     this.execute(insertProductQuery, (brand_id,name,mpn,image))
                # conn.commit()
                # logger.info(f'{vendor_id} >> Added new product with mpn "{mpn} ({this.lastrowid})".')
                # return this.lastrowid
            else:
                print("no")
                # product_id = int(records[0])
                # this.execute("UPDATE Product SET product_name = %s, product_image = %s WHERE product_id = %s", [name,image,product_id])
                # conn.commit()
                # if msrp != '':
                #     this.execute("UPDATE Product SET msrp = %s WHERE product_id = %s AND msrp IS NULL", [msrp,product_id])
                #     conn.commit()
                # logger.info(f'{vendor_id} >> Updated details for product with mpn "{mpn} ({product_id})".')
                # return product_id
    except mysql.connector.Error as e:
        logger.warning(f"{vendor_id} >> MySQL ERROR checkInsertProduct() >> {e}")
        # logger.warning(f"{vendor_id}, {brand_id}, {mpn}, {name}, {msrp}, {image}")
        return None
    finally:
        if conn.is_connected():
            conn.close()
            this.close()


if __name__ == "__main__":
    # try:
    vendor_id = 10021
    brand_id = 211
    mpn = "SN7631M#01"
    checkInsertProduct(vendor_id, brand_id, mpn)
    