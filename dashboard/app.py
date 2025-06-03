# src/dashboard/app.py

import dash
from dash import dcc, html
from dash.dependencies import Input, Output
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import pandas as pd
import os

# --- 1. Initialize Dash App ---
app = dash.Dash(__name__)
app.title = "Crypto Analysis Dashboard"

# --- 2. Load and Prepare Data ---
# Path to your processed data
PROCESSED_DATA_DIR = os.path.join(os.getcwd(), 'data', 'processed') # Adjusted path

# Available assets based on your main.py and processed files
ASSETS = {
    "BTC-USD": "Bitcoin",
    "ETH-USD": "Ethereum",
    "DOGE-USD": "Dogecoin"
}
DEFAULT_ASSET = "BTC-USD"

def load_data(asset_symbol):
    """Loads the transformed CSV data for a given asset."""
    file_path = os.path.join(PROCESSED_DATA_DIR, f"{asset_symbol}_transformed.csv")
    if os.path.exists(file_path):
        df = pd.read_csv(file_path)
        df['Date'] = pd.to_datetime(df['Date'])
        return df.sort_values(by='Date')
    return pd.DataFrame() # Return empty DataFrame if file not found

# --- 3. Define App Layout ---
app.layout = html.Div(style={'fontFamily': 'Arial, sans-serif', 'backgroundColor': '#f4f4f9', 'padding': '20px'}, children=[
    html.H1(
        "Cryptocurrency Analysis Dashboard",
        style={'textAlign': 'center', 'color': '#333'}
    ),

    html.Div(style={'marginBottom': '20px', 'textAlign': 'center'}, children=[
        html.Label("Select Cryptocurrency:", style={'marginRight': '10px', 'fontSize': '1.1em'}),
        dcc.Dropdown(
            id='crypto-dropdown',
            options=[{'label': name, 'value': symbol} for symbol, name in ASSETS.items()],
            value=DEFAULT_ASSET, # Default value
            style={'width': '50%', 'display': 'inline-block', 'verticalAlign': 'middle'}
        ),
    ]),

    # Charts and News will be updated by callbacks
    html.Div(id='charts-container'),
    html.Div(id='news-container', style={'marginTop': '30px'})
])

# --- 4. Callbacks for Interactivity ---
@app.callback(
    [Output('charts-container', 'children'),
     Output('news-container', 'children')],
    [Input('crypto-dropdown', 'value')]
)
def update_dashboard(selected_asset_symbol):
    df = load_data(selected_asset_symbol)

    if df.empty:
        return html.Div("No data available for selected asset."), html.Div()

    # --- Create Price and Volume Chart ---
    fig = make_subplots(rows=2, cols=1, shared_xaxes=True,
                        vertical_spacing=0.1,
                        subplot_titles=(f"{ASSETS[selected_asset_symbol]} Price", f"{ASSETS[selected_asset_symbol]} Volume"),
                        row_heights=[0.7, 0.3])

    # Price Candlestick or OHLC
    fig.add_trace(go.Candlestick(x=df['Date'],
                                 open=df['Open'],
                                 high=df['High'],
                                 low=df['Low'],
                                 close=df['Close'],
                                 name='Price'),
                  row=1, col=1)

    # SMA Lines (optional, uncomment to add)
    fig.add_trace(go.Scatter(x=df['Date'], y=df['SMA_Short'], mode='lines', name='SMA Short', line=dict(color='orange')), row=1, col=1)
    fig.add_trace(go.Scatter(x=df['Date'], y=df['SMA_Medium'], mode='lines', name='SMA Medium', line=dict(color='purple')), row=1, col=1)


    # Volume Bar Chart
    fig.add_trace(go.Bar(x=df['Date'], y=df['Volume'], name='Volume', marker_color='rgba(76, 175, 80, 0.7)'),
                  row=2, col=1)

    fig.update_layout(
        xaxis_rangeslider_visible=False,
        height=600,
        margin=dict(l=50, r=50, t=50, b=50),
        plot_bgcolor='white',
        paper_bgcolor='white',
        font_color='#333'
    )
    fig.update_xaxes(showgrid=True, gridwidth=1, gridcolor='LightGrey')
    fig.update_yaxes(showgrid=True, gridwidth=1, gridcolor='LightGrey')


    # --- Prepare News Display ---
    # Limiting news to latest 10 entries for brevity
    news_df = df[['Date', 'Title', 'Description']].dropna().drop_duplicates(subset=['Title']).sort_values(by='Date', ascending=False).head(10)
    news_children = [
        html.H3("Latest News", style={'color': '#333', 'borderBottom': '2px solid #ccc', 'paddingBottom': '10px'})
    ]
    for index, row in news_df.iterrows():
        news_children.append(
            html.Div(style={'backgroundColor': 'white', 'padding': '15px', 'marginBottom': '10px', 'borderRadius': '5px', 'boxShadow': '0 2px 4px rgba(0,0,0,0.1)'}, children=[
                html.H5(row['Title'], style={'marginTop': '0', 'marginBottom': '5px', 'color': '#007bff'}),
                html.P(f"{row['Date'].strftime('%Y-%m-%d')}", style={'fontSize': '0.9em', 'color': '#777', 'marginBottom': '10px'}),
                html.P(row['Description'] if pd.notna(row['Description']) else "No description available.", style={'fontSize': '1em', 'lineHeight': '1.6'})
            ])
        )

    charts_div = dcc.Graph(id='price-volume-chart', figure=fig)

    return charts_div, html.Div(news_children)


# --- 5. Run the App ---
if __name__ == '__main__':
    app.run(debug=True, port=8051) # Using port 8051 to avoid conflict if you have other Dash apps