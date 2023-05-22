import datetime
import calendar
import pdb


def get_format(frmt, res):
    date_form = ''
    i = 0
    while i < len(frmt):
        count = 1
        if frmt[i].isalpha() and frmt[i]!='C':
            count = res[frmt[i]]
            if frmt[i] =='Y' and ('C' in res.keys() or count == 4):
                date_form += '%Y'
            else:
                letter = frmt[i].lower()
                date_form += '%' + letter  
                    
        elif not frmt[i].isalpha():
            date_form += frmt[i]
            
        i = i + count
    
    return date_form


def dateproperformat(datetype , date_format):
    res = {i : date_format.count(i) for i in set(date_format)}
    date_form = get_format(date_format, res)
        
    if datetype == "Current Date":
        return datetime.date.today().strftime(date_form)
        
    if datetype == "Last day of the Month":
        month_length = calendar.monthlen(datetime.datetime.today().year , datetime.datetime.today().month)
        last_of_month = datetime.date.today().replace(day = month_length) 
        return last_of_month.strftime(date_form)
        
    if datetype == "First day of the Month":
        first_of_month = datetime.date.today().replace(day = 1)
        return first_of_month.strftime(date_form)
    
    if datetype == "First day of Next Month":
        first_of_month = datetime.date.today().replace(day = 1 , month = datetime.datetime.today().month + 1)
        return first_of_month.strftime(date_form)
    
    if datetype == "Start of current Year":
        start_date = datetime.date.today().replace(day = 1 , month = 1)
        return start_date.strftime(date_form)
    
    if datetype == "End of current Year":
        end_date = datetime.date.today().replace(day = 31 , month = 12)
        return end_date.strftime(date_form)
    
    if datetype == "Start of next Year":
        start_date = datetime.date.today().replace(day = 1 , month = 1 , year = datetime.datetime.today().year + 1)
        return start_date.strftime(date_form)
    
    if datetype == "End of next Year":
        end_date = datetime.date.today().replace(day = 31 , month = 12 , year = datetime.datetime.today().year + 1)
        return end_date.strftime(date_form)
    
    if datetype == "Next Day":
        next_day = datetime.datetime.now() + datetime.timedelta(days=1)
        return next_day.strftime(date_form)
        
