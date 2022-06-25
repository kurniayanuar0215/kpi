import mysql.connector
import pandas as pd
import os

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
IF((a.`rrc_setup_sr_attempt`-a.`rrc_setup_sr_drop`)/a.`rrc_setup_sr_attempt`*100>99,'SAFE','NOT SAFE') rrc_sr_remark,
(a.`e_rab_setup_attempt`-a.`e_rab_setup_drop`)/a.`e_rab_setup_attempt`*100 erab,
IF((a.`e_rab_setup_attempt`-a.`e_rab_setup_drop`)/a.`e_rab_setup_attempt`*100>99,'SAFE','NOT SAFE') erab_remark,
(a.`s1_attempt`-a.`s1_drop`)/a.`s1_attempt`*100 s1_sr,
IF((a.`s1_attempt`-a.`s1_drop`)/a.`s1_attempt`*100>98,'SAFE','NOT SAFE') s1_sr_remark,
(a.`cssr_attempt`-a.`cssr_drop`)/a.`cssr_attempt`*100 cssr,
IF((a.`cssr_attempt`-a.`cssr_drop`)/a.`cssr_attempt`*100>98,'SAFE','NOT SAFE') cssr_remark,
a.`s1_drop`/a.`s1_attempt` sdr,
IF(a.`s1_drop`/a.`s1_attempt`<1,'SAFE','NOT SAFE') sdr_remark,
`interference_ul_avg`,
(a.`csfb_prep_sr_attempt`-`csfb_prep_sr_drop`)/`csfb_prep_sr_attempt`*100 csfb_prep,
IF((a.`csfb_prep_sr_attempt`-`csfb_prep_sr_drop`)/`csfb_prep_sr_attempt`*100>99,'SAFE','NOT SAFE') csfb_prep_remark,
(a.`csfb_exec_sr_attempt`-`csfb_exec_sr_drop`)/a.`csfb_exec_sr_attempt`*100 csfb_exec,
IF((a.`csfb_exec_sr_attempt`-`csfb_exec_sr_drop`)/a.`csfb_exec_sr_attempt`*100>99,'SAFE','NOT SAFE') csfb_exec_remark,
a.`traffic_dl_volume_mbit`/(8*1024) py_dl_GB,
a.`traffic_ul_volume_mbit`/(8*1024) py_ul_GB,
(a.`traffic_dl_volume_mbit`+a.`traffic_ul_volume_mbit`)/(8*1024) total_py_GB,
`user_number_max`,
IF(`user_number_max`<300,'SAFE','NOT SAEF') user_number_max_remark,
`user_number_avg`,
`throughput_cell_dl_avg`/(1024) cell_dl_avg_thp,
`throughput_cell_ul_avg`/(1024) cell_ul_avg_thp,
`throughput_user_dl_avg`/(1024) user_dl_avg_thp,
`throughput_user_ul_avg`/(1024) user_ul_avg_thp,
(100-(a.radio_network_availability_rate/86400)*100) avail,
IF((100-(a.radio_network_availability_rate/86400)*100)>99,'SAFE','NOT SAFE') avail_remark,
`prb_dl_used`,
IF(`prb_dl_used`<90,'SAFE','NOT SAFE') prb_dl_used_remark,
`prb_ul_used`
FROM kpi.`kpi_daily_4g` a
LEFT JOIN test.`dapot_sitename` b
ON a.`siteid` = b.`site_id`
WHERE a.tanggal BETWEEN "'''+date_1+'''" AND "'''+date_2+'''" '''+query_siteid+'''
GROUP BY a.`tanggal`,a.`cellname`;
'''

name_file = 'KPI_'+date_1+'_'+date_2+'.xlsx'

kpi_4g = pd.read_sql(q_4g, conn)

with pd.ExcelWriter('''F:/KY/kpi/download/'''+name_file) as writer:
    kpi_4g.to_excel(
        writer, index=False, sheet_name='KPI_4G')

try:
    update.message.bot.sendDocument(update.message.chat.id, open(
        'F:/KY/kpi/download/'+name_file, 'rb'))
except:
    os.environ["https_proxy"] = "https://10.59.66.1:8080"
    os.system("telegram-send --file F:/KY/kpi/download/"+name_file+"")
