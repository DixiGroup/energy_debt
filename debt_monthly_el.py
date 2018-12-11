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
    
    # get month, company, and oblast
    
    current_month_index = debtdf.loc[pd.notnull(debtdf['consumer_type']) & debtdf['consumer_type'].str.contains('року')].index[0]
    current_month = debtdf['consumer_type'][current_month_index].replace(' за ', '').replace(' року', '')
    current_month
    
    company = debtdf['consumer_type'][current_month_index+1]
    if(pd.isnull(company)):
        company = debtdf['consumer_type'].iloc[10]

    oblast = sheet_name.split(sep = '_')[0]
    
    # remove unnecessary rows after heat (get second index for Споживачі)
    
    debtdf = debtdf.reset_index(drop = True)
     
    # slice relevant rows
    
    debtdf_el = debtdf.iloc[13:32,:]
    debtdf_el = debtdf_el.loc[~debtdf_el['consumer_type'].str.contains('тому числі')]

    # filter redundant rows & create general consumer category var
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
                                 oblast = oblast,
                                 year = str.split(current_month, sep = ' ')[1],
                                 month = int(months_lookup['months_dig'][months_lookup['months_ukr'] == str.split(current_month, sep = ' ')[0]]))
    
    # move newly created vars forward / rearrange
    cols = debtdf_el.columns.tolist()
    cols = cols[-2:]+cols[-3:-2]+cols[-4:-3]+cols[-5:-4]+cols[:-5]
    debtdf_el = debtdf_el[cols]
    
    return debtdf_el

def load_all_sheets(filename):
    filepath = os.path.join('raw data', filename)
    xlfile = pd.ExcelFile(filepath)
    sheet_names_series = pd.Series(xlfile.sheet_names)
    cond = ~sheet_names_series.str.contains('!|сього|ТЕЦ|тепло|_оплата')
    sheet_names_to_read = sheet_names_series[cond][:-1]
    
    list_ = []
    for sheet in sheet_names_to_read:
        print(sheet)
        df = parse_single_sheet(xlfile, sheet)
        list_.append(df)
    frame = pd.concat(list_)
    return(frame)

# create and save data frame for all files, all sheets in 'raw data' folder
raw_data_files = os.listdir('raw data')

list_el = []
for fi in raw_data_files:
    print(fi)
    df = load_all_sheets(fi)
    list_el.append(df)

frame_el = pd.concat(list_el).sort_values(by = ['year', 'month'])
frame_el['company'][frame_el['company'].isnull()] = frame_el['oblast'][frame_el['company'].isnull()]

frame_el_only = frame_el.loc[~frame_el['company'].str.contains('ТЕЦ|тепло')]

# create folder open data if it doesn't exist
od_folder = 'open data' 
if not os.path.exists(od_folder):
    os.makedirs(od_folder)

frame_el_only.to_excel(os.path.join('open data', 'debt_el_full.xlsx'), index = False)
frame_el_only.to_csv(os.path.join('open data', 'debt_el_full.csv'), index = False)