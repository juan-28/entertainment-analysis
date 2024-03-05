import pandas as pd
import dash
from dash import dcc, html, dash_table
from dash.dependencies import Input, Output
import dash_bootstrap_components as dbc
import plotly.express as px

# Load the dataset
df = pd.read_csv(
    "/Users/pranavsukumaran/Desktop/Personal_dev/netflix/project/data/final_merged_data.csv"
)

# Combine date columns into a single datetime column
df["Combined Date"] = pd.to_datetime(df[["Year", "Month", "Day", "Hour"]])

# Create Year-Month column for easier filtering
df["Year-Month"] = df["Combined Date"].dt.to_period("M")

# Split genres into individual genres
df["Split Genres"] = df["genres"].str.split(",")
df = df.explode("Split Genres")

# Unique options for dropdowns
year_options = [{"label": year, "value": year} for year in df["Year"].unique()]
month_options = [{"label": month, "value": month} for month in df["Month"].unique()]
profile_options = [
    {"label": profile, "value": profile} for profile in df["Profile Name"].unique()
]
genre_options = [
    {"label": genre, "value": genre} for genre in df["Split Genres"].unique()
]
title_type_options = [
    {"label": title_type, "value": title_type}
    for title_type in df["titleType"].unique()
]
app = dash.Dash(__name__, external_stylesheets=[dbc.themes.BOOTSTRAP])  # Apply theme
app.layout = dbc.Container(
    [
        dbc.Row(
            [
                dbc.Col(
                    [
                        html.H1(
                            "Netflix Viewing Activity Dashboard",
                            className="text-center mb-4",
                        ),
                    ],
                    width=12,
                ),
            ]
        ),
        dbc.Row(
            [
                dbc.Col(
                    [
                        html.Div(
                            [
                                html.Label("Select Profile:", className="form-label"),
                                dcc.Dropdown(
                                    id="profile-dropdown",
                                    options=profile_options,
                                    value=profile_options[0]["value"],
                                    className="mb-2",
                                ),
                                html.Label("Select Genre:", className="form-label"),
                                dcc.Dropdown(
                                    id="genre-dropdown",
                                    options=genre_options,
                                    value="All Genres",
                                    className="mb-2",
                                ),
                                html.Label(
                                    "Select Title Type:", className="form-label"
                                ),
                                dcc.Dropdown(
                                    id="title-type-dropdown",
                                    options=title_type_options,
                                    value="All Types",
                                    className="mb-2",
                                ),
                                html.Label("Select Year:", className="form-label"),
                                dcc.Dropdown(
                                    id="year-dropdown",
                                    options=year_options,
                                    value=year_options[0]["value"],
                                    className="mb-2",
                                ),
                                html.Label("Select Month:", className="form-label"),
                                dcc.Dropdown(
                                    id="month-dropdown",
                                    options=month_options,
                                    multi=True,
                                    className="mb-2",
                                ),
                            ],
                            className="p-3 border bg-light",
                        ),
                    ],
                    width=4,
                ),
                dbc.Col(
                    [
                        dcc.Graph(id="times-watched-graph", className="mb-4"),
                        dcc.Graph(id="rating-graph"),
                    ],
                    width=8,
                ),
            ]
        ),
    ],
    fluid=True,
    className="mt-4",
)


@app.callback(
    [
        Output("times-watched-graph", "figure"),
        Output("rating-graph", "figure"),
        Output("genre-dropdown", "options"),
    ],  # Update genre options too
    [
        Input("profile-dropdown", "value"),
        Input("genre-dropdown", "value"),
        Input("title-type-dropdown", "value"),
        Input("year-dropdown", "value"),
        Input("month-dropdown", "value"),
    ],
)
def update_graphs(
    selected_profile,
    selected_genre,
    selected_title_type,
    selected_year,
    selected_months,
):
    # Start with the full dataset
    filtered_df = df.copy()

    # Apply filters incrementally
    if selected_profile:
        filtered_df = filtered_df[filtered_df["Profile Name"] == selected_profile]
    if selected_genre and selected_genre != "All Genres":  # Check for 'All Genres'
        filtered_df = filtered_df[filtered_df["Split Genres"] == selected_genre]
    if selected_title_type and selected_title_type != "All Types":
        filtered_df = filtered_df[filtered_df["titleType"] == selected_title_type]
    if selected_year:
        filtered_df = filtered_df[filtered_df["Year"] == selected_year]
    if selected_months:
        filtered_df = filtered_df[filtered_df["Month"].isin(selected_months)]

    # Times Watched Analysis
    times_watched = (
        filtered_df.groupby("primaryTitle").size().reset_index(name="Times Watched")
    )
    fig_times_watched = px.bar(
        times_watched,
        x="primaryTitle",
        y="Times Watched",
        title="Times Watched per Title",
    )

    # IMDb Rating Analysis
    avg_rating = (
        filtered_df.groupby("primaryTitle").agg({"averageRating": "mean"}).reset_index()
    )
    fig_avg_rating = px.bar(
        avg_rating,
        x="primaryTitle",
        y="averageRating",
        title="Average IMDb Rating per Title",
    )

    return fig_times_watched, fig_avg_rating, genre_options


if __name__ == "__main__":
    app.run_server(debug=True)
