import mysql.connector
import pandas as pd
from sqlalchemy import create_engine
import os
from datetime import datetime, timedelta

date = datetime.today() - timedelta(days=1)
strdate = date.strftime("%Y-%m-%d")

# DB CONFIG
conn = mysql.connector.connect(
    host="10.47.150.144",
    user="sqaviewjbr",
    password="5qAv13wJbr@2019#",
    database="kpi"
)
# QUERY
q_4g = '''SELECT 
a.`tanggal`,
a.`cellname`,
a.`siteid`,
b.`kabupaten`,
b.`kecamatan`,
b.`kabkec`,
b.`cluster`,
b.`rtp`,
b.`nsa`,
b.`long`,
b.`lat`,
(a.`rrc_setup_sr_attempt`-a.`rrc_setup_sr_drop`)/a.`rrc_setup_sr_attempt`*100 rrc_sr,
(a.`e_rab_setup_attempt`-a.`e_rab_setup_drop`)/a.`e_rab_setup_attempt`*100 erab,
(a.`s1_attempt`-a.`s1_drop`)/a.`s1_attempt`*100 s1_sr,
(a.`cssr_attempt`-a.`cssr_drop`)/a.`cssr_attempt`*100 cssr,
a.`s1_drop`/a.`s1_attempt` sdr,
`interference_ul_avg`,
(a.`csfb_prep_sr_attempt`-`csfb_prep_sr_drop`)/`csfb_prep_sr_attempt`*100 csfb_prep,
(a.`csfb_exec_sr_attempt`-`csfb_exec_sr_drop`)/a.`csfb_exec_sr_attempt`*100 csfb_exec,
a.`traffic_dl_volume_mbit`/(8*1024) py_dl_GB,
a.`traffic_ul_volume_mbit`/(8*1024) py_ul_GB,
(a.`traffic_dl_volume_mbit`+a.`traffic_ul_volume_mbit`)/(8*1024) total_py_GB,
`user_number_max`,
`user_number_avg`,
`throughput_cell_dl_avg`/(1024) cell_dl_avg_thp,
`throughput_cell_ul_avg`/(1024) cell_ul_avg_thp,
`throughput_user_dl_avg`/(1024) user_dl_avg_thp,
`throughput_user_ul_avg`/(1024) user_ul_avg_thp,
(100-(a.radio_network_availability_rate/86400)*100) avail,
`prb_dl_used`,
`prb_ul_used`
FROM kpi.`kpi_daily_4g` a
JOIN test.`dapot_sitename` b
ON a.`siteid` = b.`site_id`
WHERE a.`tanggal` BETWEEN "'''+strdate+'''" AND "'''+strdate+'''"
GROUP BY a.`tanggal`,a.`cellname`;
'''
# GET
name_file = 'kpi.csv'
kpi_4g = pd.read_sql(q_4g, conn)
kpi_4g.to_csv('F:\KY\kpi\export\kpi.csv', index=False)

# UPDATE
engine = create_engine('mysql://sunandar:qwerty123@10.3.107.87/kpi_rf')

df = pd.read_csv("F:\KY\kpi\export\kpi.csv", sep=',',
                 quotechar='\'', encoding='utf8')
try:
    df.to_sql('4g_kpi_daily', con=engine,
              index=False, if_exists='append')
    print('Update Done')
except:
    print('Already Updated')

dir = 'F:\KY\kpi\export'
for f in os.listdir(dir):
    os.remove(os.path.join(dir, f))
