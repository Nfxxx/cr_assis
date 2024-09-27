from bokeh.plotting import figure,show, output_file
from bokeh.models import ColumnDataSource
from bokeh.plotting import figure,show
from bokeh.models import ColumnDataSource, DatetimeTickFormatter, HoverTool
from bokeh.models.widgets import Panel, Tabs
from bokeh.models import LinearAxis, Range1d
from bokeh.transform import dodge
import numpy as np
import pandas as pd

def bar(result, x_axis_label = "", y_axis_label = "", title = "", width = 0.1, play = True):
    TOOLS = '''
            pan,            
            box_zoom,
            wheel_zoom, xwheel_zoom, ywheel_zoom,   
            crosshair,
            save,reset,help
            '''
    hover = HoverTool(tooltips=[('value','$y{0.0000}'),('name','$name')],
                formatters={"@x": 'printf'})
    colors = {0:"#e84d60", 1:"#718dbf", 2:"#c9d9d3", 3: "red", 4:"orange", 5:"yellow", 6:"green",
            7:"blue",8:"black"}
    indexes = result.index.tolist()   
    cols = result.columns.tolist()    
    p = figure(plot_width = 900, plot_height = 600, 
            tools = TOOLS,  
            toolbar_location='above',     # "above"，"below"，"left"，"right"
            x_axis_label = x_axis_label, y_axis_label = x_axis_label,    # X,Y轴label
            x_range=indexes,
            title= title ,                       
            )
    data = {'index':indexes} 
    for col in cols:
        data[col] = result[col].tolist()
    source = ColumnDataSource(data = data)
    
    locations = np.linspace(-0.25, 0.25, len(cols))
    
    for i in range(len(cols)):
        p.vbar(x=dodge('index', locations[i], range=p.x_range), top=cols[i], width=width, source=source,color=colors[i], legend_label=cols[i], name = cols[i]) #用dodge的方法把3个柱状图拼到了一起

    # 绘制多系列柱状图       0.25和width=0.2是柱状图之间的空隙间隔，都是0.2了就没有空隙了
    # dodge(field_name, value, range=None) → 转换成一个可分组的对象，value为元素的位置（配合width设置）
    # value(val, transform=None) → 按照年份分为dict

    p.xgrid.grid_line_color = None
    p.legend.location = "top_right"
    p.legend.orientation = "horizontal"
    p.legend.background_fill_alpha = 0.2 
    p.legend.click_policy="hide" 
    if play:
        show(p)
    return p

def line(result, x_axis_label = "", y_axis_label = "", title = "", x_axis_type = "datetime", play = True, 
        plot_width = 900, plot_height = 600, tips = [], formatters = [], tags = []):
    #result: index-datetime
    TOOLS = '''
        pan,            
        box_zoom,
        wheel_zoom, xwheel_zoom, ywheel_zoom,   
        crosshair,
        save,reset,help
        '''
    data = {}
    data["x"] = list(result.index.values).copy()
    cols = []
    for col in result.columns:
        data[col] = list(result[col].values).copy()
        if col not in tags:
            cols.append(col)
    source = ColumnDataSource(data = data)
    if len(tips) == 0 and len(formatters) == 0 and len(tags) == 0:
        if x_axis_type == "datetime":
            hover = HoverTool(tooltips=[('datetime','@x{%Y-%m-%d %H:%M:%S}'),('value','$y{0.000000}'),('name','$name')],
                        formatters={"@x": "datetime"})
        elif x_axis_type == "linear":
            hover = HoverTool(tooltips=[('x', '$x{0.0000}'), ('value','$y{0.000000}'),('name','$name')],
                        formatters={"@x": "printf"})
    else:
        hover = HoverTool(tooltips = tips, formatters = formatters)
    colors = ["black", "red", "olive", "darkred", "skyblue", "orange", "salmon","navy","maroon",'dimgrey']
    #colors = ["red", "olive", "darkred", "goldenrod", "skyblue", "orange", "salmon"]
    #不同符号的散点图
    # asterisk(), circle(), circle_cross(), circle_x(), cross(), diamond(), diamond_cross(), inverted_triangle()
    # square(), square_cross(), square_x(), triangle(), x()
    
    if result.shape[1] < len(colors):
        p1 = figure(plot_width = plot_width, plot_height = plot_height, 
            tools = TOOLS,  
            toolbar_location='above',     # "above"，"below"，"left"，"right"
            x_axis_type=x_axis_type,
            x_axis_label = x_axis_label, y_axis_label = y_axis_label,    
            title= title                        
            )
        p1.add_tools(hover)
        p1.xaxis.axis_label_text_font = "song" 
        p1.yaxis.axis_label_text_font = "song" 
        p1.xaxis.axis_label_text_font_style = "normal" 
        p1.yaxis.axis_label_text_font_style = "normal"
        p1.xgrid.grid_line_color = None
        p1.ygrid.grid_line_color = None
        num = 0
        p1.y_range = Range1d(start = result.dropna().values.min(),
                                end = result.dropna().values.max())
        for col in cols:
            p1.line(result.index, result[col], legend_label=col, line_color=colors[num],name = col, line_width = 2)
            num = num + 1
        p1.legend.background_fill_alpha = 0.2 
        p1.legend.click_policy="hide" 
        if play:
            show(p1)
        return p1
    else:
        tabs = []
        num_tab = int(np.ceil(result.shape[1]/len(colors)))
        for i in range(num_tab):
            p1 = figure(plot_width = plot_width, plot_height = plot_height, 
                tools = TOOLS, 
                toolbar_location='above',     
                x_axis_type=x_axis_type,
                x_axis_label = x_axis_label, y_axis_label = y_axis_label,    
                title= title                        
                )
            p1.add_tools(hover)
            p1.xaxis.axis_label_text_font = "song" 
            p1.yaxis.axis_label_text_font = "song"
            p1.xaxis.axis_label_text_font_style = "normal" 
            p1.yaxis.axis_label_text_font_style = "normal" 
            p1.xgrid.grid_line_color = None
            p1.ygrid.grid_line_color = None
            num = 0
            while num < len(colors) and i*len(colors) + num < result.shape[1]:
                col = cols[i*len(colors) + num]
                p1.line(result.index, result[col], legend_label=col, line_color=colors[num],name = col, line_width = 2)
                num = num + 1
            p1.legend.background_fill_alpha = 0.2 
            p1.legend.click_policy="hide" 
            tab = Panel(child = p1, title = f"subtitle {str(i+1)}")
            tabs.append(tab)
        t = Tabs(tabs = tabs)
        if play:
            show(t)
        return tabs
    
    
def line_doubleY(result, right_columns, left_columns = [], x_axis_label = "", y_axis_label = "", title = "", x_axis_type = "datetime", play = True, plot_width = 900, plot_height = 600, tips = [], formatters = [], tags = []):
    #result: index-datetime
    #right_columns : columns on the right, list
    TOOLS = '''
        pan,            
        box_zoom,
        wheel_zoom, xwheel_zoom, ywheel_zoom,   
        crosshair,
        save,reset,help
        '''
    data = {}
    data["x"] = list(result.index.values).copy()
    cols = []
    for col in result.columns:
        data[col] = list(result[col].values).copy()
        if col not in tags:
            cols.append(col)
    source = ColumnDataSource(data = data)
    if len(tips) == 0 and len(formatters) == 0 and len(tags) == 0:
        if x_axis_type == "datetime":
            hover = HoverTool(tooltips=[('datetime','@x{%Y-%m-%d %H:%M:%S}'),('value','$y{0.000000}'),('name','$name')],
                          formatters={"@x": "datetime"})
        elif x_axis_type == "linear":
            hover = HoverTool(tooltips=[('x', '$x{0.0000}'), ('value','$y{0.000000}'),('name','$name')],
                          formatters={"@x": "printf"})
    else:
        hover = HoverTool(tooltips = tips, formatters = formatters)
    colors = ["black", "red", "green", "orange", "skyblue", "yellow", "salmon","navy","maroon",'dimgrey', "olive", "cyan", "lime",
             "violet", "tomato", "springgreen", "gray", "indigo", "aquamarine", "burlywood", "chartreuse", "cornflowerblue",
             "lightslategray", "orchid", "pink", "saddlebrown", "slateblue", "wheat", "turquoise", "silver"]
    #colors = ["red", "olive", "darkred", "goldenrod", "skyblue", "orange", "salmon"]
    # asterisk(), circle(), circle_cross(), circle_x(), cross(), diamond(), diamond_cross(), inverted_triangle()
    # square(), square_cross(), square_x(), triangle(), x()
    
    p1 = figure(plot_width = plot_width, plot_height = plot_height, # 图表宽度、高度
           tools = TOOLS,  
           toolbar_location='above',     # "above"，"below"，"left"，"right"
           x_axis_type=x_axis_type,
           x_axis_label = x_axis_label, y_axis_label = y_axis_label,    
           title= title                        
          )
    p1.add_tools(hover)
    p1.xaxis.axis_label_text_font = "song" 
    p1.yaxis.axis_label_text_font = "song" 
    p1.xaxis.axis_label_text_font_style = "normal" 
    p1.yaxis.axis_label_text_font_style = "normal"
    p1.xgrid.grid_line_color = None
    p1.ygrid.grid_line_color = None
    num = 0
    if len(left_columns) == 0:
        columns = cols.copy()
        for col in right_columns:
            if col in columns:
                columns.remove(col)
            else:
                print(f"{col} doesn't belong to result.columns")
        left_columns = columns
    range_num = pd.DataFrame(columns = left_columns, index = ["min", "max"])
    for col in left_columns:
        range_num.loc["min", col] = min(result[col].dropna().values) if not np.isnan(result[col]).sum() == len(result[col]) else 0
        range_num.loc["max", col] = max(result[col].dropna().values) if not np.isnan(result[col]).sum() == len(result[col]) else 1
    p1.y_range = Range1d(start = float(min(range_num.loc["min"].values)),
                                end = float(max(range_num.loc["max"].values)))
    
    for col in left_columns:
        p1.line(result.index, result[col], legend_label=col, line_color=colors[num],name = col, line_width = 2)
        num = num + 1
    #p1.yaxis[0].formatter = NumeralTickFormatter(format='00.0000%')
    #添加新的y轴
    range_num = pd.DataFrame(columns = right_columns, index = ["min", "max"])
    y_column2_range = 'settle_range'
    for col in right_columns:
        range_num.loc["min", col] = min(result[col].dropna().values) if not np.isnan(result[col]).sum() == len(result[col]) else 0
        range_num.loc["max", col] = max(result[col].dropna().values) if not np.isnan(result[col]).sum() == len(result[col]) else 1
    p1.extra_y_ranges = {
        y_column2_range:Range1d(start = float(min(range_num.loc["min"].dropna().values)),
                                end = float(max(range_num.loc["max"].dropna().values)))
                        }
    p1.add_layout(LinearAxis(y_range_name = y_column2_range),'right')
    for col in right_columns:
        p1.line(result.index, result[col], legend_label=col, line_color=colors[num],name = col, y_range_name=y_column2_range, line_width = 2)
        num = num + 1

    p1.legend.background_fill_alpha = 0.2 
    p1.legend.click_policy="hide" 
    if play:
        show(p1)
    return p1

def line_triY(result, left_columns, right_columns = [], x_axis_label = "", y_axis_label = "", title = "", x_axis_type = "datetime", play = True, plot_width = 900, plot_height = 600, tips = [], formatters = [], tags = []):
    #result: index-datetime
    #right_columns : columns on the right, list
    TOOLS = '''
        pan,            
        box_zoom,
        wheel_zoom, xwheel_zoom, ywheel_zoom,   
        crosshair,
        save,reset,help
        '''
    data = {}
    data["x"] = list(result.index.values).copy()
    cols = []
    for col in result.columns:
        data[col] = list(result[col].values).copy()
        if col not in tags:
            cols.append(col)
    source = ColumnDataSource(data = data)
    if len(tips) == 0 and len(formatters) == 0 and len(tags) == 0:
        if x_axis_type == "datetime":
            hover = HoverTool(tooltips=[('datetime','@x{%Y-%m-%d %H:%M:%S}'),('value','$y{0.000000}'),('name','$name')],
                          formatters={"@x": "datetime"})
        elif x_axis_type == "linear":
            hover = HoverTool(tooltips=[('x', '$x{0.0000}'), ('value','$y{0.000000}'),('name','$name')],
                          formatters={"@x": "printf"})
    else:
        hover = HoverTool(tooltips = tips, formatters = formatters)
    colors = ["black", "red", "green", "orange", "skyblue", "yellow", "salmon","navy","maroon",'dimgrey', "olive", "cyan", "lime",
             "violet", "tomato", "springgreen", "gray", "indigo", "aquamarine", "burlywood", "chartreuse", "cornflowerblue",
             "lightslategray", "orchid", "pink", "saddlebrown", "slateblue", "wheat", "turquoise", "silver"]
    #colors = ["red", "olive", "darkred", "goldenrod", "skyblue", "orange", "salmon"]
    # asterisk(), circle(), circle_cross(), circle_x(), cross(), diamond(), diamond_cross(), inverted_triangle()
    # square(), square_cross(), square_x(), triangle(), x()
    
    p1 = figure(plot_width = plot_width, plot_height = plot_height, # 图表宽度、高度
           tools = TOOLS,  
           toolbar_location='above',     # "above"，"below"，"left"，"right"
           x_axis_type=x_axis_type,
           x_axis_label = x_axis_label, y_axis_label = y_axis_label,    
           title= title                        
          )
    p1.add_tools(hover)
    p1.xaxis.axis_label_text_font = "song" 
    p1.yaxis.axis_label_text_font = "song" 
    p1.xaxis.axis_label_text_font_style = "normal" 
    p1.yaxis.axis_label_text_font_style = "normal"
    p1.xgrid.grid_line_color = None
    p1.ygrid.grid_line_color = None
    num = 0
    if len(right_columns) == 0:
        columns = cols.copy()
        for col in left_columns:
            if col in columns:
                columns.remove(col)
            else:
                print(f"{col} doesn't belong to result.columns")
        right_columns = columns
    range_num = pd.DataFrame(columns = left_columns, index = ["min", "max"])
    for col in left_columns:
        range_num.loc["min", col] = min(result[col].dropna().values) if not len(result[col].astype(float).dropna()) == 0 else 0
        range_num.loc["max", col] = max(result[col].dropna().values) if not len(result[col].astype(float).dropna()) == 0 else 1
        if range_num.loc["min", col] != range_num.loc["max", col]:
            p1.y_range = Range1d(start = float(range_num.loc["min", col]),
                                        end = float(range_num.loc["max", col]))
        else:
            p1.y_range = Range1d(start = float(range_num.loc["min", col]),
                                end = (float(range_num.loc["max", col])+1))
    
    for col in left_columns:
        p1.line(result.index, result[col], legend_label=col, line_color=colors[num],name = col, line_width = 2)
        num = num + 1
    #p1.yaxis[0].formatter = NumeralTickFormatter(format='00.0000%')
    #添加新的y轴
    range_num = pd.DataFrame(columns = right_columns, index = ["min", "max"])
    p1.extra_y_ranges = {}
    for col in right_columns:
        y_column2_range = str(num)
        range_num.loc["min", col] = min(result[col].dropna().values) if not len(result[col].astype(float).dropna()) == 0 else 0
        range_num.loc["max", col] = max(result[col].dropna().values) if not len(result[col].astype(float).dropna()) == 0 else 1
        if range_num.loc["min", col] != range_num.loc["max", col]:
            p1.extra_y_ranges[y_column2_range] = Range1d(start = float(range_num.loc["min", col]),
                                        end = float(range_num.loc["max", col]))
        else:
            p1.extra_y_ranges[y_column2_range] = Range1d(start = float(range_num.loc["min", col]),
                                end = (float(range_num.loc["max", col])+1))
        p1.add_layout(LinearAxis(y_range_name = y_column2_range),'right')
        p1.line(result.index, result[col], legend_label=col, line_color=colors[num],name = col, y_range_name=y_column2_range, line_width = 2)
        num = num + 1
    p1.legend.background_fill_alpha = 0.2 
    p1.legend.click_policy="hide" 
    if play:
        show(p1)
    return p1