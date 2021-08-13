--==============================================================
-- PACKAGE st_layer.pkg_dim_datetime_st:                    BODY
--==============================================================

CREATE OR REPLACE PACKAGE BODY st_layer.pkg_dim_datetime_st
-- Package Load data from storage level to ST level
--
AS

--Procedure to load data of t_sa_datetime from SA_* level to ST_* level
    PROCEDURE load_datetime
    AS
    BEGIN
        
        DECLARE
                
            TYPE curs_type IS REF CURSOR; 
            
            select_stmt              VARCHAR2(1000);        
            src_curs                 curs_type;
            num_curs                 NUMBER;           
            temp                     INTEGER;
            
            TYPE date_arr            IS TABLE OF st_layer.t_st_datetime.order_dt%TYPE;
            dates_arr date_arr;
            
            TYPE arr_order_dt        IS TABLE OF st_layer.t_st_datetime.order_dt%TYPE;
            TYPE arr_year_cal        IS TABLE OF st_layer.t_st_datetime.year_calendar%TYPE;
            TYPE arr_month_num       IS TABLE OF st_layer.t_st_datetime.month_number%TYPE;
            TYPE arr_day_name        IS TABLE OF st_layer.t_st_datetime.day_name%TYPE;
            TYPE arr_day_month       IS TABLE OF st_layer.t_st_datetime.day_number_month%TYPE;
            
            order_dt_var             arr_order_dt;
            year_cal_var             arr_year_cal; 
            month_num_var            arr_month_num;
            day_name_var             arr_day_name;
            day_month_var            arr_day_month;
        
        BEGIN
        
            select_stmt := 'SELECT order_dt, year_calendar, month_number, day_name, day_number_month FROM sa_datetime.t_sa_datetime';
            
            num_curs := DBMS_SQL.OPEN_CURSOR;            
            DBMS_SQL.PARSE(num_curs, 
                           select_stmt, 
                           DBMS_SQL.NATIVE);            
            temp := DBMS_SQL.EXECUTE(num_curs);            
            src_curs := DBMS_SQL.TO_REFCURSOR(num_curs);
                 
            LOOP                 
                FETCH src_curs
                    INTO order_dt_var
                                     ,year_cal_var
                                     ,month_num_var
                                     ,day_name_var
                                     ,day_month_var;
                                 
                FORALL i IN dates_arr.FIRST..dates_arr.LAST
                    INSERT INTO st_layer.t_st_datetime
                        VALUES (order_dt_var(i)
                               ,year_cal_var(i)
                               ,month_num_var(i)
                               ,day_name_var(i)
                               ,day_month_var(i) );                                                
                EXIT WHEN src_curs%NOTFOUND;  
            END LOOP;
        COMMIT;
        END;            
    END;
END;
/

execute pkg_dim_datetime_st.load_datetime;
select * from st_layer.t_st_datetime;

