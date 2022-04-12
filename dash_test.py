import dash

import dash_html_components as html
import dash_core_components as dcc
from dash.dependencies import Input, Output, State
from plotly import graph_objs as go

animals=['giraffes', 'orangutans', 'monkeys']
cars = ['Jetta','Lambo','Truck']

fig = go.Figure([go.Bar(x=animals, y=[20, 14, 23])])

# required first
app = dash.Dash(
    __name__,
    meta_tags=[{"name": "viewport", "content": "width=device-width"}],
    suppress_callback_exceptions=False
)
app.title = "IEX Dashboard"
server = app.server

#Live html components first
app.layout = html.Div(
    children=[
        dcc.RadioItems(
            id='graph selection radio',
            options=[
                {'label': 'Animal Graph', 'value': 'Animal'},
                {'label': 'Car Graph', 'value': 'Cars'},
            ],
            value='Animal'
        ),
        html.Div(
            id='graph container',
            children=[
                'Div Child',
                dcc.Graph(
                    id='graph itself',
                    figure=go.Figure(
                        [go.Bar(x=animals, y=[20, 14, 23])]
                    ),
                ),
            ]
        ),
        "Sample text 2",
    ]
)

#Callbacks second
#Outputs: an output can only exist in one callback

@app.callback(
    [Output('graph container','children')],
    Input('graph selection radio','value'),
)
def update_graph(selected_radio_button):
    if selected_radio_button == 'Animal':
        output = [
            'Div Child - animals',
            dcc.Graph(
                id='graph itself',
                figure=go.Figure(
                    [go.Bar(x=animals, y=[20, 14, 23])]
                ),
            ),
        ]
    elif selected_radio_button == 'Cars':
        output = [
            'Div Child - cars',
            dcc.Graph(
                figure=go.Figure(
                    [go.Bar(x=cars, y=[200, 140, 230])]
                ),
            ),
        ]

    return [output]

#required last
if __name__ == "__main__":
    app.run_server(debug=True)