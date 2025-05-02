from pymodbus.client.sync import ModbusTcpClient
import mysql.connector
from datetime import datetime
import time

client = ModbusTcpClient('10.14.60.252')
client.connect()

def decode_signed_16bit(value):
    if value & 0x8000:
        return -((~value + 1) & 0xFFFF)
    else:
        return value

def read_group_data(start_address):
    modbus_address = start_address - 30001
    result = client.read_input_registers(modbus_address, 6, unit=2)
    if not result.isError():
        regs = result.registers
        print(f"Registers read: {regs}")
        
        ctl = regs[0]
        op = regs[1]
        sp_raw = decode_signed_16bit(regs[2])
        tm_raw = decode_signed_16bit(regs[4])

        data = {
            'room_temp_c': tm_raw / 10.0,
            'setpoint_c': sp_raw / 10.0,
            'fan_speed': (ctl >> 12) & 0x7,
            'fan_direction': (ctl >> 8) & 0x7,
            'thermo_status': (ctl >> 7) & 0x1,
            'heater_status': (ctl >> 6) & 0x1,
            'fan_status': (ctl >> 5) & 0x1,
            'normal_operation': (ctl >> 3) & 0x1,
            'forced_off_status': (ctl >> 2) & 0x1,
            'on_off_status': ctl & 0x1,
            'cool_heat_master': (op >> 14) & 0x3,
            'defrost_hot': (op >> 13) & 0x1,
            'operation_status': (op >> 8) & 0xF,
            'filter_sign': (op >> 4) & 0xF,
            'operation_mode': op & 0xF
        }
        return data
    else:
        print(f"Error reading registers starting at {start_address}")
        return None

db = mysql.connector.connect(
    host="localhost",
    user="azzikri",
    password="Irkizza12",
    database="DAIKIN_SBM"
)
cursor = db.cursor()

def insert_data(table_name, data):
    """Menyimpan data ke dalam tabel MySQL."""
    placeholders = ', '.join(['%s'] * len(data))
    columns = ', '.join(data.keys())
    sql = f"INSERT INTO {table_name} (ts, {columns}) VALUES (NOW(), {placeholders})"
    values = list(data.values())
    cursor.execute(sql, values)
    db.commit()

while True:
    for i in range(50):
        base_address = 32001 + 6 * i
        unit = i // 16 + 1
        sub = i % 16
        if unit == 1 and sub in [0,1]:
            lantai = 1
        if unit == 1 and sub in [2,3,4,5,6,7,8]:
            lantai = 2
        if unit == 1 and sub in [9,10,11,12,13,14,15]:
            lantai = 3
        if unit == 2 and sub in [0,1,2,3,4,5]:
            lantai = 3
        if unit == 2 and sub in [6,7,8,9,10,11,12,13,14,15]:
            lantai = 4
        if unit == 3 and sub in [0,1]:
            lantai = 4
        if unit == 3 and sub in [2,3,4,5,6,7,8]:
            lantai = 5
        if unit == 3 and sub in [9,10,11,12,13,14,15]:
            lantai = 6
        if unit == 4 and sub in [0,1]:
            lantai = 6

        table_name = f"{unit}{str(sub).zfill(2)}_F{lantai}"

        data = read_group_data(base_address)
        if data:
            insert_data(table_name, data)
    time.sleep(5)
