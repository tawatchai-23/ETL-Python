import requests
import xml.etree.ElementTree as ET
import psycopg2

# URL ของ API
url = "https://data.tmd.go.th/api/Station/v1/?uid=demo&ukey=demokey"

# ดึงข้อมูล XML จาก API
response = requests.get(url)
if response.status_code == 200:
    xml_data = response.content  # ข้อมูล XML ในรูป binary
    # แปลง XML เป็น ElementTree
    root = ET.fromstring(xml_data)

    # ดึงข้อมูลจาก <header>
    header = root.find('header')
    header_data = {
        "title": header.findtext("title"),
        "description": header.findtext("description"),
        "uri": header.findtext("uri"),
        "last_build_date": header.findtext("lastBuildDate"),
        "copyright": header.findtext("copyRight"),
        "generator": header.findtext("generator"),
        "status": header.findtext("status")
    }

    # ดึงข้อมูลจาก <Station>
    stations = []
    for station in root.findall('Station'):
        # Helper function: ดึงค่าและแปลงเป็น float
        def get_value_and_unit(element, default_value=None):
            if element is not None:
                return float(element.text) if element.text else default_value
            return default_value

        station_data = {
            "StationID": station.findtext("StationID"),
            "WmoCode": station.findtext("WmoCode"),
            "StationNameThai": station.findtext("StationNameThai"),
            "StationNameEnglish": station.findtext("StationNameEnglish"),
            "StationType": station.findtext("StationType"),
            "Province": station.findtext("Province"),
            "ZipCode": station.findtext("ZipCode"),
            "Latitude": float(station.findtext("Latitude")) if station.findtext("Latitude") else None,
            "Longitude": float(station.findtext("Longitude")) if station.findtext("Longitude") else None,
            "HeightAboveMSL": get_value_and_unit(station.find("HeightAboveMSL"), None),
            "HeightofWindWane": get_value_and_unit(station.find("HeightofWindWane"), None),
            "HeightofBarometer": get_value_and_unit(station.find("HeightofBarometer"), None),
            "HeightofThermometer": get_value_and_unit(station.find("HeightofThermometer"), None),
            # ข้อมูลจาก <header>
            "HeaderTitle": header_data["title"],
            "HeaderDescription": header_data["description"],
            "HeaderUri": header_data["uri"],
            "HeaderLastBuildDate": header_data["last_build_date"],
            "HeaderCopyRight": header_data["copyright"],
            "HeaderGenerator": header_data["generator"],
            "HeaderStatus": header_data["status"]
        }
        stations.append(station_data)

    # เชื่อมต่อ PostgreSQL
    conn = psycopg2.connect(
        host="your host",
        database="your database",  # แก้ไขชื่อฐานข้อมูล
        user="your user",  # แก้ไขชื่อผู้ใช้
        password="your password"  # แก้ไขรหัสผ่าน
    )
    cur = conn.cursor()

    # แทรกข้อมูลลงใน PostgreSQL
    for station in stations:
        sql = """
        INSERT INTO <your table> (
            title, description, uri, last_build_date, copyright, generator, status,
            station_id, wmo_code, station_name_thai, station_name_english, station_type, 
            province, zip_code, latitude, longitude, geom, height_above_msl, height_of_wind_vane,
            height_of_barometer, height_of_thermometer
        ) VALUES (
            %(HeaderTitle)s, %(HeaderDescription)s, %(HeaderUri)s, %(HeaderLastBuildDate)s, 
            %(HeaderCopyRight)s, %(HeaderGenerator)s, %(HeaderStatus)s,
            %(StationID)s, %(WmoCode)s, %(StationNameThai)s, %(StationNameEnglish)s, %(StationType)s,
            %(Province)s, %(ZipCode)s, %(Latitude)s, %(Longitude)s, 
            ST_SetSRID(ST_MakePoint(%(Longitude)s, %(Latitude)s), 4326), 
            %(HeightAboveMSL)s, %(HeightofWindWane)s, %(HeightofBarometer)s, %(HeightofThermometer)s
        )
        """
        cur.execute(sql, station)

    # Commit และปิดการเชื่อมต่อ
    conn.commit()
    cur.close()
    conn.close()

    print("ข้อมูลถูกเพิ่มลงในฐานข้อมูลเรียบร้อยแล้ว")
else:
    print(f"Error fetching data: {response.status_code}")
