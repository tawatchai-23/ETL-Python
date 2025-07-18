import requests
import xml.etree.ElementTree as ET
import psycopg2

def safe_float(value):
    try:
        return float(value.strip())
    except (ValueError, AttributeError):
        return None

# URL API
url = "https://data.tmd.go.th/api/RainRegions/v1/?uid=api&ukey=api12345"

# ดึงข้อมูล XML จาก API
response = requests.get(url)
if response.status_code == 200:
    xml_data = response.content
    root = ET.fromstring(xml_data)

    # ดึงข้อมูล Header
    header = root.find(".//Header")
    header_data = {
        "title": header.findtext("Title"),
        "description": header.findtext("Description"),
        "uri": header.findtext("Uri"),
        "last_build_date": header.findtext("LastBuildDate"),
        "date_of_data": header.findtext("DateOfData"),
        "copyright": header.findtext("CopyRight"),
        "generator": header.findtext("Generator"),
        "status": header.findtext("status"),
    }

    # เชื่อมต่อ PostgreSQL
    conn = psycopg2.connect(
        dbname="your dbname",
        user="your user",
        password="your password",
        host="your host",
        port="your port"
    )
    cur = conn.cursor()

    # ดึงข้อมูล Regions และ Stations
    for region in root.findall(".//Region"):
        region_name = region.findtext("RegionName")
        for province in region.findall(".//Province"):
            province_name = province.findtext("ProvinceName")
            for station in province.findall(".//Station"):
                station_data = {
                    **header_data,  # เพิ่มข้อมูล Header
                    "region_name": region_name,
                    "province_name": province_name,
                    "station_name": station.findtext("StationNameThai"),
                    "latitude": safe_float(station.find("Latitude").text),
                    "longitude": safe_float(station.find("Longitude").text),
                    "rainfall": safe_float(station.find("Rainfall").text),
                }

                # Insert ข้อมูลลงใน rainregions
                cur.execute("""
                    INSERT INTO <your table> (title, description, uri, last_build_date, date_of_data, copyright, generator, status,
                                             region_name, province_name, station_name, latitude, longitude, rainfall)
                    VALUES (%(title)s, %(description)s, %(uri)s, %(last_build_date)s, %(date_of_data)s, %(copyright)s, 
                            %(generator)s, %(status)s, %(region_name)s, %(province_name)s, %(station_name)s, 
                            %(latitude)s, %(longitude)s, %(rainfall)s);
                """, station_data)

    # Commit และปิดการเชื่อมต่อ
    conn.commit()
    cur.close()
    conn.close()

    print("Data has been inserted successfully!")
else:
    print(f"Error fetching data: {response.status_code}")
