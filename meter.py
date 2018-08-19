import sqlite3
from sqlite3 import Error
import datetime

def create_conn(db_file):
    try:
        conn=sqlite3.connect(db_file)
        return conn
    except Error as e:
        print(e)

    return None

def select_all_meter(conn):
    """
    SELECT all dinstinct meter id from db
    """
    cur=conn.cursor()
    cur.execute("select DISTINCT meter from meter")
    return cur.fetchall()

def select_meter_all_records_shorted(meter,conn):
    """SELECT a meter all record and sort by seqno """
    cur=conn.cursor()
    cur.execute("select Date_in_meter,Connection_Status from meter where meter=? order by Sequence_id ASC",meter)
    #cur.execute("select Date_in_meter,Connection_Status from meter where meter='0010388C' order by Sequence_id ASC")
    return cur.fetchall()
    

def main():
    dataBase="meter.db"
    conn=create_conn(dataBase)
    with conn:
        """Get All meters in list then iterate all meters one by
        Then get intividual date for the meter and get the last meter activity of the day
        """
        meters=select_all_meter(conn)
        for meter in meters:
            print("======================")
            print(meter[0])
            print("======================")
            dates=select_meter_all_records_shorted(meter,conn)
            final_time_perDay={}
            date_list=[]
            """Filter the last time stamp for the day and insert into to dist"""
            for dt in dates:
                obj_cur_dt=datetime.datetime.strptime(dt[0],'%d-%m-%Y %H:%M')
                cur_dt=obj_cur_dt.strftime('%m-%d-%Y')
                final_time_perDay[cur_dt] = dt[1]
            """Get the date into list to compare it"""
            date_list=list(final_time_perDay.keys())
            for x in range(0,len(date_list)-1):
                date_diff=datetime.datetime.strptime(date_list[x+1],'%m-%d-%Y')- datetime.datetime.strptime(date_list[x],'%m-%d-%Y')
                """Time gap between two disconncet is more then 1 day then there meter didn't conncet that day"""
                if date_diff.days>1 and (final_time_perDay[date_list[x]]=='Disconnected' and  final_time_perDay[date_list[x]]=='Disconnected'):
                    for num in range(1,date_diff.days):
                        print(datetime.datetime.strptime(date_list[x],'%m-%d-%Y')+datetime.timedelta(days=num))
        
        

        
        
            





if __name__=='__main__':
    main()