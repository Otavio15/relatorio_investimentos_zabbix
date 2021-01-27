import pandas as pd
from pandas_datareader import data as wb
from datetime import date
from pyzabbix import ZabbixAPI, ZabbixAPIException, ZabbixSender, ZabbixMetric
import sys, time

TEMPO = 15
IP = '192.168.71.129'
ZABBIX_SERVER = 'http://{}/zabbix'.format(IP)
zapi = ZabbixAPI(ZABBIX_SERVER, user='Admin', password='zabbix')
host_name = 'app1'
hosts = zapi.host.get(filter={"host": host_name}, selectInterfaces=["interfaceid"])
tikers = {'nacionais':[{'AGRO3.SA':2},{'ALPA4.SA':2},{'BIOM3.SA':2},{'GGBR4.SA':1},{'GMAT3.SA':27},{'GOLL4.SA':1},{'ITSA4.SA':1},{'JBSS3.SA':3},{'JHSF3.SA':23},{'MGLU3.SA':8},{'MXRF11.SA':7},{'PETR4.SA':3},{'POMO4.SA':2},{'SLCE3.SA':5},{'WEGE3.SA':2}],'internacionais':[{'TSLA':0.259},{'RKT':1},{'AMZN':0.0281},{'BABA':0.5296},{'FB':0.2222},{'GOOGL':0.035},{'NVDA':0.0772},{'STNE':1},{'VEA':0.2282}],'criptomoedas':[{'BTC-USD':0.0001},{'ETH-USD':0.0},{'XRP-USD':0.0},{'LTC-USD':0.0}]}
dia = date.today()

### Adiciona os Itens no zabbix ###
for ativo in tikers.keys():
    print(ativo)
    for tipo_do_ativo in tikers[ativo]:
        for nome_do_ativo in tipo_do_ativo.keys():
            print('Tiker: {}'.format(nome_do_ativo))
            try:    
                if hosts:
                    host_id = hosts[0]["hostid"]
                    print("Found host id {0}".format(host_id))
                    try:
                        item = zapi.item.create(
                            hostid=host_id,
                            name=str(nome_do_ativo),
                            key_=str(nome_do_ativo),
                            type=2,
                            value_type=0,
                            interfaceid=hosts[0]["interfaces"][0]["interfaceid"],
                            delay=30,
                            description=str(ativo),
                            units=' R$'
                        )
                    except ZabbixAPIException as e:
                        print(e)
                        sys.exit()
                    print("Added item with itemid {0} to host: {1}".format(item["itemids"][0], host_name))
                else:
                    print("No hosts found")
            except:
                print('Houve um problema, host não adicionado')
            
            time.sleep(1)
#######

###### Joga as informações no zabbix ######

df_internacionais = pd.DataFrame()
df_nacionais = pd.DataFrame()
df_cripto = pd.DataFrame()

tikers_nacionais = []
tikers_internacionais = []
tikers_cripto = []

for t in tikers['nacionais']:
    for k in t.keys():
        tikers_nacionais.append(k)

for t in tikers['internacionais']:
    for k in t.keys():
        tikers_internacionais.append(k)

for t in tikers['criptomoedas']:
    for k in t.keys():
        tikers_cripto.append(k)

try:
    zbx = ZabbixSender(zabbix_server=IP, zabbix_port=10051, use_config=None)
except:
    print('Problemas ao se conectar com o servidor zabbix')

while True:

    try:
        metrics = []

        for t in tikers_internacionais:
            df_internacionais[t] = wb.DataReader(t, data_source='yahoo', start=dia)['Adj Close']

        for t in tikers_nacionais:
            df_nacionais[t] = wb.DataReader(t, data_source='yahoo', start=dia)['Adj Close']

        for t in tikers_cripto:
            df_cripto[t] = wb.DataReader(t, data_source='yahoo', start=dia)['Adj Close']

        total_n = 0
        total_i = 0
        total_c = 0
        reserva_usd = 7.95
        reserva_brl = 1151.36

        acoes = {}
        dolar = wb.DataReader('USDBRL=X', data_source='yahoo', start=date.today())['Adj Close'][-1]

        ################################################################
        aux = []
        count = 0
        soma = 0
        for tiker in df_internacionais:
            for v in tikers['internacionais'][count].values():
                aux.append({tiker: round(df_internacionais[tiker][0]*dolar*v, 2)})
                count += 1
                soma += df_internacionais[tiker][0]*dolar*v

        total_i = soma
        aux.append({'TOTAL BRL': round(soma, 2)})
        acoes['internacionais'] = aux

        metrics.append(ZabbixMetric('app1', 'tikerinternacionais', acoes['internacionais'][-1]['TOTAL BRL'])) ############

        ################################################################

        ################################################################
        aux = []
        count = 0
        soma = 0
        for tiker in df_nacionais:
            for v in tikers['nacionais'][count].values():
                aux.append({tiker: round(df_nacionais[tiker][0]*v, 2)})
                count += 1
                soma += df_nacionais[tiker][0]*v

        total_n = soma
        aux.append({'TOTAL BRL': round(soma, 2)})
        acoes['nacionais'] = aux

        metrics.append(ZabbixMetric('app1', 'tikernacionais', acoes['nacionais'][-1]['TOTAL BRL'])) ############

        ################################################################

        ################################################################
        aux = []
        count = 0
        soma = 0
        for tiker in df_cripto:
            for v in tikers['criptomoedas'][count].values():
                aux.append({tiker: round((df_cripto[tiker][0]*v*dolar), 2)})
                count += 1
                soma += df_cripto[tiker][0]*v*dolar

        total_c = soma
        aux.append({'TOTAL BRL': round(soma, 2)})
        acoes['criptomoedas'] = aux

        metrics.append(ZabbixMetric('app1', 'tikercripto', acoes['criptomoedas'][-1]['TOTAL BRL'])) ############

        ################################################################

        acoes['Dolar'] = round(dolar, 2)
        acoes['Reserva USD'] = round(reserva_usd*dolar,2)
        acoes['Reserva BRL'] = reserva_brl
        acoes['TOTAL BRL'] = round(total_n + total_i + total_c + (reserva_usd*dolar) + reserva_brl)

        acoes['Percent. Acoes Nacionais'] = round(acoes['nacionais'][-1]['TOTAL BRL'] * 100 / acoes['TOTAL BRL'], 2)
        acoes['Percent. Acoes Internacionais'] = round(acoes['internacionais'][-1]['TOTAL BRL'] * 100 / acoes['TOTAL BRL'], 2)
        acoes['Percent. Cripto'] = round(acoes['criptomoedas'][-1]['TOTAL BRL'] * 100 / acoes['TOTAL BRL'], 2)
        acoes['Percent. Reserva USD'] = round(acoes['Reserva USD'] * 100 / acoes['TOTAL BRL'], 2)
        acoes['Percent. Reserva BRL'] = round(acoes['Reserva BRL'] * 100 / acoes['TOTAL BRL'], 2)

        metrics.append(ZabbixMetric('app1', 'capitaltotal', acoes['TOTAL BRL'])) ############

        metrics.append(ZabbixMetric('app1', 'reservausd', acoes['Reserva USD'])) ############
        metrics.append(ZabbixMetric('app1', 'reservabrl', reserva_brl)) ############

        metrics.append(ZabbixMetric('app1', 'percentinernacionais', acoes['Percent. Acoes Internacionais'])) ############
        metrics.append(ZabbixMetric('app1', 'percentnacionais', acoes['Percent. Acoes Nacionais'])) ############
        metrics.append(ZabbixMetric('app1', 'percentcripto', acoes['Percent. Cripto'])) ############
        metrics.append(ZabbixMetric('app1', 'percentdolar', acoes['Percent. Reserva USD'])) ############
        metrics.append(ZabbixMetric('app1', 'percentreal', acoes['Percent. Reserva BRL'])) ############


        for tipo in tikers.keys():
            count = 0
            for acao in tikers[tipo]:
                for chave in acao.keys():
                    metrics.append(ZabbixMetric('app1', chave, acoes[tipo][count][chave]))
                    count += 1

        zbx.send(metrics)
        print('Dados enviados ao servidor! \n Total BRL: {}, Total Reserva BRL: {}, Total Reserva USD in BRL {}'.format(acoes['TOTAL BRL'],acoes['Reserva BRL'],acoes['Reserva USD']))
        print('Percent.Nacionais: {}, Percent.Internacionais: {}, Percent.Cripto: {}, Percent.USD: {}, Percent.BRL: {}'.format(acoes['Percent. Acoes Nacionais'],acoes['Percent. Acoes Internacionais'],acoes['Percent. Cripto'],acoes['Percent. Reserva USD'],acoes['Percent. Reserva BRL']))
        time.sleep(TEMPO)
    except:
        
        print('Deu pau')
