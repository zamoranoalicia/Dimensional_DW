CREATE OR REPLACE PROCEDURE  FILL_STAGE_EMPLOYEES
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

    EXECUTE IMMEDIATE 'TRUNCATE TABLE STG_EMPLOYEES';

   INSERT INTO  STG_EMPLOYEES(EMPLOYEEID,
                               LASTNAME,
                               FIRSTNAME,
                               TITLE,
                               TITLEOFCOURTESY,
                               BIRTHDATE,
                               HIREDATE,
                               ADDRESS,
                               CITY,
                               REGION,
                               POSTALCODE,
                               COUNTRY,
                               HOMEPHONE,
                               EXTENSION,
                               NOTES,
                               REPORTSTO)
    SELECT EMPLOYEEID,
           LASTNAME,
           FIRSTNAME,
           TITLE,
           TITLEOFCOURTESY,
           BIRTHDATE,
           HIREDATE,
           ADDRESS,
           CITY,
           REGION,
           POSTALCODE,
           COUNTRY,
           HOMEPHONE,
           EXTENSION,
           TO_LOB(NOTES),
           REPORTSTO
           FROM ORDERS_DB.EMPLOYEES;


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