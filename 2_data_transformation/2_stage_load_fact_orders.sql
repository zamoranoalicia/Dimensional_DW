CREATE OR REPLACE PROCEDURE  LOAD_FACT_ORDERS
IS
    V_PROCESS_NAME          VARCHAR2(30) := $$plsql_unit;
    V_START_DATE            DATE;
    V_END_DATE              DATE;
    V_PROCESS_MESSAGE       VARCHAR2(255) := 'Process '|| $$plsql_unit || ' run successfully';
    V_QUANTITY_ROWS         NUMBER(10) := 0;
    V_EXECUTED_SUCCESS      VARCHAR2(1) := 'Y';
    v_NUMBER_OF_DIFFERENCES NUMBER(10) :=0;

BEGIN
    V_START_DATE := SYSDATE;

    EXECUTE IMMEDIATE 'TRUNCATE TABLE STG_FACT_ORDERS';

    INSERT INTO STG_FACT_ORDERS(ORDERID,
                                CUSTOMERID,
                                EMPLOYEEID,
                                ORDERDATE,
                                REQUIREDDATE,
                                SHIPPEDDATE,
                                SHIPVIA,
                                FREIGHT,
                                SHIPNAME,
                                UNITPRICE,
                                QUANTITY,
                                DISCOUNT,
                                CUSTOMER_IDW,
                                EMPLOYEE_IDW,
                                PRODUCT_IDW,
                                IDW_SHIPPERS,
                                GEOGRAFIA_IDW,
                                FECHA_CARGA)
    SELECT ORD.ORDERID,
           ORD.CUSTOMERID,
           ORD.EMPLOYEEID,
           ORD.ORDERDATE,
           ORD.REQUIREDDATE,
           ORD.SHIPPEDDATE,
           ORD.SHIPVIA,
           ORD.FREIGHT,
           ORD.SHIPNAME,
           ORD_D.UNITPRICE,
           ORD_D.QUANTITY,
           ORD_D.DISCOUNT,
           DIM_CUST.CUSTOMER_IDW,
           DIM_EMP.EMPLOYEE_IDW,
           DIM_PROD.PRODUCT_IDW,
           DIM_SHIP.IDW_SHIPPERS,
           DIM_GEO.GEOGRAFIA_IDW,
           SYSDATE
           FROM ORDERS_DB.ORDERS ORD
               JOIN ORDERS_DB.ORDERDETAILS ORD_D ON ORD.ORDERID = ORD_D.ORDERID
               JOIN STG_DIM_CUSTOMERS DIM_CUST ON ORD.CUSTOMERID = DIM_CUST.CUSTOMERID
               JOIN STG_DIM_EMPLOYEES DIM_EMP ON ORD.EMPLOYEEID = DIM_EMP.EMPLOYEEID
               JOIN STG_DIM_PRODUCTS DIM_PROD  ON ORD_D.PRODUCTID = DIM_PROD.PRODUCTID
               JOIN STG_DIM_SHIPPERS DIM_SHIP ON DIM_SHIP.SHIPPERID = ORD.SHIPVIA
               JOIN STG_DIM_GEOGRAFIA DIM_GEO ON ORD.SHIPADDRESS = DIM_GEO.ADRESS
           WHERE ORD.SHIPPEDDATE IS NOT NULL;

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