CREATE OR REPLACE PROCEDURE  LOAD_DIM_PRODUCTS
IS
    V_PROCESS_NAME          VARCHAR2(30) := $$plsql_unit;
    V_START_DATE            DATE;
    V_END_DATE              DATE;
    V_PROCESS_MESSAGE       VARCHAR2(255) := 'Process '|| $$plsql_unit || ' run successfully';
    V_QUANTITY_ROWS         NUMBER(10) := 0;
    V_EXECUTED_SUCCESS      VARCHAR2(1) := 'Y';
    v_NUMBER_OF_DIFFERENCES NUMBER(10) := 0 ;
    V_TABLE_NAME            VARCHAR2(30) := 'STG_DIM_PRODUCTS';
    V_TABLE_ID_NAME         VARCHAR2(30) := 'PRODUCT_IDW';

BEGIN
    V_START_DATE := SYSDATE;

    EXECUTE IMMEDIATE 'TRUNCATE TABLE STG_DIM_PRODUCTS';

    CREATE_SEQUENCE(V_TABLE_NAME);

    CREATE_TRIGGER_ON_INSERT(V_TABLE_NAME, V_TABLE_ID_NAME);

    INSERT INTO  STG_DIM_PRODUCTS(PRODUCTID,
                                  PRODUCTNAME,
                                  QUANTITYPERUNIT,
                                  UNITPRICE,
                                  UNITSINSTOCK,
                                  UNITSONORDER,
                                  REORDERLEVEL,
                                  DISCONTINUED,
                                  CATEGORYID,
                                  CATEGORYNAME,
                                  DESCRIPTION_CATEGORY,
                                  SUPPLIERID,
                                  SUPPLIER,
                                  SUPPLIERCONTACT,
                                  SUPPLIERTITLE,
                                  ADDRES,
                                  CITY,
                                  REGION,
                                  POSTALCODE,
                                  COUNTRY,
                                  PHONE,
                                  FAX)
    SELECT PROD.PRODUCTID,
           PROD.PRODUCTNAME,
           PROD.QUANTITYPERUNIT,
           PROD.UNITPRICE,
           PROD.UNITSINSTOCK,
           PROD.UNITSONORDER,
           PROD.REORDERLEVEL,
           PROD.DISCONTINUED,
           CAT.CATEGORYID,
           CAT.CATEGORYNAME,
           CAT.DESCRIPTION,
           SUPP.SUPPLIERID,
           SUPP.COMPANYNAME,
           SUPP.CONTACTNAME,
           SUPP.CONTACTTITLE,
           SUPP.ADDRESS,
           SUPP.CITY,
           SUPP.REGION,
           SUPP.POSTALCODE,
           SUPP.COUNTRY,
           SUPP.PHONE,
           SUPP.FAX
    FROM ORDERS_DB.PRODUCTS PROD
        JOIN ORDERS_DB.CATEGORIES CAT ON PROD.CATEGORYID = CAT.CATEGORYID
        JOIN ORDERS_DB.SUPPLIERS SUPP ON PROD.SUPPLIERID = SUPP.SUPPLIERID;

    V_END_DATE := SYSDATE;
    V_QUANTITY_ROWS := SQL%ROWCOUNT;

    SAVE_PROCESS_LOG(V_PROCESS_NAME, V_START_DATE, V_END_DATE,  V_QUANTITY_ROWS,V_PROCESS_MESSAGE, V_EXECUTED_SUCCESS,v_NUMBER_OF_DIFFERENCES);

    COMMIT;
EXCEPTION
    WHEN OTHERS THEN
        V_END_DATE := SYSDATE;
        V_PROCESS_MESSAGE := ('There were and error while executing ' || $$plsql_unit || ' procedure '||SQLCODE||' '||SQLERRM|| ' ');
        V_EXECUTED_SUCCESS := 'N';
        SAVE_PROCESS_LOG(V_PROCESS_NAME, V_START_DATE, V_END_DATE,  V_QUANTITY_ROWS,V_PROCESS_MESSAGE, V_EXECUTED_SUCCESS,v_NUMBER_OF_DIFFERENCES);
END;