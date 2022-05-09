CREATE OR REPLACE PROCEDURE CREATE_TRIGGER_ON_INSERT(
    TABLE_NAME IN VARCHAR2,
    TABLE_ID IN VARCHAR2
)
AS
BEGIN
    EXECUTE IMMEDIATE 'CREATE OR REPLACE TRIGGER ' || TABLE_NAME || '_ON_INSERT' ||' '||
                      'BEFORE INSERT ON ' || TABLE_NAME || ' ' ||
                      'FOR EACH ROW ' ||
                      'BEGIN' ||' '||
                      'SELECT' || ' ' || TABLE_NAME || '_ID_SEQUENCE' || '.NEXTVAL' || ' ' ||
                      'INTO :new.' || TABLE_ID || ' ' ||
                      'FROM dual;' || ' ' ||
                      'END;';
    COMMIT;
END;
