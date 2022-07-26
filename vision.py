"""

выводит диаграмму в виде BAR в оффлайн режиме

"""

from plotly import offline
import plotly.graph_objs as po
import plotly.express as px
import pandas as pd


def vision_price():
    filename = 'testRes.csv'
    df = pd.read_csv(filename)
    data = px.bar(df, x='срок поставки', y='стоимость,$')

    x_axis_conf = {'title': 'Дата поставки'}
    y_axis_conf = {'title': 'Стоимость $'}

    m_layout = po.Layout(title='____________________________', xaxis=x_axis_conf, yaxis=y_axis_conf)
    offline.plot({'data': data, 'layout': m_layout}, filename='diagram.html')
