import os
import pandas as pd

# lookup for months
months_ukr = ["січень", "лютий", "березень", "квітень", "травень", "червень",
              "липень", "серпень", "вересень", "жовтень", "листопад", "грудень"]
seqn = lambda a, b: list(range(a, b+1))
months_dig = seqn(1, 12)
list_keys = ['months_ukr', 'months_dig']
list_values = [months_ukr, months_dig]
months_lookup = pd.DataFrame(dict(list(zip(list_keys, list_values))))
del(months_ukr, months_dig, list_keys, list_values)

def parse_single_sheet(xlfile, sheet_name, energy_type = 'el'):
    # Load a sheet into a DataFrame
    df = xlfile.parse(sheet_name)
    
    debtdf = df.iloc[:, [0,1,2,3,4,5,7,9,10]]
    debtdf.columns = ['consumer_type', 'debt_year_start', 'debt_month_start', 
                      'consumption_tkwth', 'consumption_tuah', 'paid_month_tuah',
                      'paid_month_money_tuah', 'debt_correction_tuah',
                      'debt_month_end']
    
    # get month and company
    
    current_month_index = debtdf.loc[pd.notnull(debtdf['consumer_type']) & debtdf['consumer_type'].str.contains('року')].index[0]
    current_month = debtdf['consumer_type'][current_month_index].replace(' за ', '').replace(' року', '')
    current_month
    
    company = debtdf['consumer_type'][current_month_index+1]
    
    # remove unnecessary rows after heat (get second index for Споживачі)
    
    debtdf = debtdf.reset_index(drop = True)
    index_trim1 = debtdf['consumer_type'][debtdf['consumer_type'] == 'Споживачі'].index[0]
    index_trim2 = debtdf['consumer_type'][debtdf['consumer_type'] == 'Споживачі'].index[1]
    debtdf = debtdf.iloc[(index_trim1+1):index_trim2,:]
     
    # split into el and heat (get second index for Всього)
    
    debtdf = debtdf.reset_index(drop = True)
    firstcol = list(debtdf['consumer_type'])
    idx_total = []
    for idx, val in enumerate(firstcol):
        if('Всього' in str(val)):
            idx_total = idx_total + [idx-1]
    
    if(pd.isnull(company)):
        company = debtdf['consumer_type'][idx_total[0]]
    
    if(pd.notnull(debtdf['consumer_type'][idx_total[1]])):
        company_heat = debtdf['consumer_type'][idx_total[1]]
    else:
        company_heat = company
    
    idx_to_split = idx_total[1]+1
    
    debtdf_el = debtdf.iloc[:idx_to_split,:]
    debtdf_heat = debtdf.iloc[idx_to_split:,:]    
    
    # filter redundant rows & create general consumer category var
    debtdf_el = debtdf_el[debtdf_el['consumer_type'].notnull()]
    debtdf_el = debtdf_el[debtdf_el['consumer_type'] != 1]
    debtdf_el = debtdf_el[debtdf_el['consumer_type'] != company]
    debtdf_el = debtdf_el[debtdf_el['consumer_type'] != company_heat]
    debtdf_el = debtdf_el[~debtdf_el['consumer_type'].str.contains("сього|тому числі", na = False)]
    
    debtdf_el.loc[debtdf_el['consumer_type'].str.contains("\d."), 'consumer_cat'] = debtdf_el.loc[debtdf_el['consumer_type'].str.contains("\d."), 'consumer_type']    
    debtdf_el['consumer_cat'] = debtdf_el['consumer_cat'].ffill().str.replace('\d.|,', '').str.lower().str.strip() 
    debtdf_el = debtdf_el[debtdf_el['consumer_type'] != '1.Промисловість']
    
    # replace values which are not really sums
    debtdf_el.iloc[8, 1:9] = debtdf_el.iloc[8, 1:9]-debtdf_el.iloc[9, 1:9]
    debtdf_el.iloc[-3, 1:9] = debtdf_el.iloc[-3, 1:9]-debtdf_el.iloc[-2, 1:9]-debtdf_el.iloc[-1, 1:9]

    # swap rows for them
    a,b = debtdf_el.iloc[8].copy(), debtdf_el.iloc[9].copy()
    debtdf_el.iloc[8],debtdf_el.iloc[9] = b,a
    
    a,b,c = debtdf_el.iloc[-3].copy(), debtdf_el.iloc[-2].copy(), debtdf_el.iloc[-1].copy()
    debtdf_el.iloc[-3], debtdf_el.iloc[-2], debtdf_el.iloc[-1] = b,c,a
    
    debtdf_el = debtdf_el.reset_index(drop = True)
    debtdf_el['consumer_type'] = ['вугільна промисловість',
         'металургійна промисловість',
         'хімічна промисловість',
         'машинобудівна промисловість',
         'газова промисловість',
         'інша промисловість',
         'залізниця',
         'сільгоспспоживачі',
         'водоканали',
         'житлокомунгосп (інше)',
         'підприєм. та організ. державного бюджету',
         'підприєм. та організ.  місцевого бюджету',
         'населення (без урах. пільг та субсидій)',
         'населення (пільги)',
         'населення (субсидії)',
         'інші споживачі']
    
    # assign company and month vars
    debtdf_el = debtdf_el.assign(company = company, 
                                 year = str.split(current_month, sep = ' ')[1],
                                 month = int(months_lookup['months_dig'][months_lookup['months_ukr'] == str.split(current_month, sep = ' ')[0]]))
    
    # move newly created vars forward / rearrange
    cols = debtdf_el.columns.tolist()
    cols = cols[-2:]+cols[-3:-2]+cols[-4:-3]+cols[:-4]
    debtdf_el = debtdf_el[cols]
    
    # repeat for heat df (attention, other indices)
    debtdf_heat = debtdf_heat[debtdf_heat['consumer_type'].notnull()]
    debtdf_heat = debtdf_heat[debtdf_heat['consumer_type'] != 1]
    debtdf_heat = debtdf_heat[~debtdf_heat['consumer_type'].str.contains("сього|тому числі", na = False)]
    
    debtdf_heat.loc[debtdf_heat['consumer_type'].str.contains('\d.'), 'consumer_cat'] = debtdf_heat.loc[debtdf_heat['consumer_type'].str.contains('\d.'), 'consumer_type']
    debtdf_heat['consumer_cat'] = debtdf_heat['consumer_cat'].ffill().str.replace('\d.|,', '').str.lower().str.strip()
    debtdf_heat = debtdf_heat[debtdf_heat['consumer_type'] != '2. Житлокомунгосп,']
    
    debtdf_heat = debtdf_heat.reset_index(drop = True)
    index_to_exclude = debtdf_heat[debtdf_heat['consumer_type'].str.contains('3.')].index[0]+1
    debtdf_heat = debtdf_heat.iloc[:index_to_exclude,:]
    debtdf_heat = debtdf_heat.reset_index(drop = True)
    debtdf_heat['consumer_type'] = ['промисловість',
     'населення',
     'держ.бюджет',
     'місц.бюджет',
     'житлокомунгосп (інші)',
     'інші споживачі']
    
    company = company_heat
    del(company_heat)
    debtdf_heat = debtdf_heat.assign(company = company, 
                                     year = str.split(current_month, sep = ' ')[1],
                                     month = int(months_lookup['months_dig'][months_lookup['months_ukr'] == str.split(current_month, sep = ' ')[0]]))
    cols = debtdf_heat.columns.tolist()
    cols = cols[-2:]+cols[-3:-2]+cols[-4:-3]+cols[:-4]
    debtdf_heat = debtdf_heat[cols]
    
    if(energy_type == 'el'):
        return debtdf_el
    else:
        return debtdf_heat

def load_all_sheets(filename, energy_type = 'el'):
    filepath = os.path.join('raw data', filename)
    xlfile = pd.ExcelFile(filepath)
    sheet_names_series = pd.Series(xlfile.sheet_names)
    cond = ~sheet_names_series.str.contains('!|сього')
    sheet_names_to_read = sheet_names_series[cond][:-1]
    
    list_ = []
    for sheet in sheet_names_to_read:
        df = parse_single_sheet(xlfile, sheet, energy_type = energy_type)
        list_.append(df)
    frame = pd.concat(list_)
    return(frame)

# create and save data frame for all files, all sheets in 'raw data' folder
raw_data_files = os.listdir('raw data')

list_el = []
for fi in raw_data_files:
    df = load_all_sheets(fi, energy_type = 'el')
    list_el.append(df)
frame_el = pd.concat(list_el).sort_values(by = ['year', 'month'])

# create folder open data if it doesn't exist
od_folder = 'open data' 
if not os.path.exists(od_folder):
    os.makedirs(od_folder)

frame_el.to_excel(os.path.join('open data', 'debt_el_full.xlsx'), index = False)
frame_el.to_csv(os.path.join('open data', 'debt_el_full.csv'), index = False)

# the same for heat - currently disabled until the data gets validated

#list_heat = []
#for fi in raw_data_files:
#    df = load_all_sheets(fi, energy_type = 'heat')
#    list_heat.append(df)
#frame_heat = pd.concat(list_heat).sort_values(by = ['year', 'month'])
#    
#frame_heat.to_excel('open data\\debt_heat_full.xlsx', index = False)
#frame_heat.to_csv('open data\\debt_heat_full.csv', index = False)
