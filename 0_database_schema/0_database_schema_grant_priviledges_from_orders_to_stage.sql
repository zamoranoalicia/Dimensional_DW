SELECT 'GRANT SELECT ON '||TABLE_NAME||' TO STAGE;' FROM USER_TABLES;

GRANT SELECT ON CATEGORIES TO STAGE;
GRANT SELECT ON CUSTOMERS TO STAGE;
GRANT SELECT ON EMPLOYEES TO STAGE;
GRANT SELECT ON ORDERDETAILS TO STAGE;
GRANT SELECT ON ORDERS TO STAGE;
GRANT SELECT ON PRODUCTS TO STAGE;
GRANT SELECT ON SHIPPERS TO STAGE;
GRANT SELECT ON SUPPLIERS TO STAGE;