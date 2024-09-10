import pandas as pd
import seaborn as sb
from crawler import newest_update, sqlalchemy_connect
import os
import aiofiles
import aiohttp
import asyncio
from PIL import Image
from io import BytesIO
import base64
from jinja2 import Environment, FileSystemLoader
from datetime import datetime
from weasyprint import HTML, CSS
import pyemoji
import colorcet as cc
import matplotlib.dates as mdates
import matplotlib.pyplot as plt

REGENERATE_AFTER_SECONDS = 1
pd.options.mode.chained_assignment = None 


## Fetching data
# Calls crawler to fetch latest top 20 memes from reddit API
async def get_newest_update():
    timestamp = await newest_update()
    return timestamp

# Pulls latest top 20 meme's name, title, url, thumnail url, upvotes and downvotes from database (for report's table), stores in pandas dataframe
def get_top_memes_data_from_db(engine):
    select_script = '''
        SELECT t1.name, t1.title, t1.url, t1.thumbnail_url, t2.upvotes, t2.downvotes
        FROM memes AS t1
        JOIN (
            SELECT name, upvotes, downvotes
            FROM votes
            WHERE crawled_at IN (
                SELECT MAX(crawled_at) FROM votes
            )
        ) AS t2 ON t1.name = t2.name
    '''
    df = pd.read_sql_query(select_script, con=engine)
    return df

# Pulls latest top 20 meme's upvote and downvote histories from database (for reports graph), stores in pandas dataframe
def get_upvote_time_series_of_top_memes(engine):
    select_script = '''
        SELECT t1.name, t2.title, t1.upvotes, t1.downvotes, t1.crawled_at
        FROM votes AS t1
        JOIN memes AS t2 ON t1.name = t2.name
        WHERE t1.name in (
            SELECT name
            FROM votes
            WHERE crawled_at IN (
                SELECT MAX(crawled_at) FROM votes
            )
        ) 
        ORDER BY t1.crawled_at
    '''

    df = pd.read_sql_query(select_script, con=engine)
    return df


## Caching data
# Fetch images from thumbnail url and cache them in file system
async def cache_img(df):
    # Create image cache path if not exist
    img_cache_dir = os.path.join(os.path.dirname(os.path.dirname(__file__)), "img_cache")
    if not os.path.exists(img_cache_dir):
        os.mkdir(img_cache_dir)
    
    # Check if all images in top 20 memes are cached
    cached_img = os.listdir(img_cache_dir)
    cached_img_name = [name.rsplit(".", 1)[0] for name in cached_img]
    uncached_img_df = df[~df["name"].isin(cached_img_name)]
    old_imgs = list(set(cached_img_name) - set(df.name) - set(["nsfw", "default", "chart"]))

    # Fetch uncached images
    print("Caching images ...")
    for index, row in uncached_img_df.iterrows():
        url = row["thumbnail_url"]
        name = row["name"]
        extension = url.rsplit(".", 1)[-1].split("?", 1)[0]
        if url == "nsfw":
            continue

        async with aiohttp.ClientSession() as session:
            async with session.get(url) as resp:
                if resp.status == 200:
                    new_img_dir = os.path.join(img_cache_dir, f"{name}.{extension}")
                    f = await aiofiles.open(new_img_dir, mode='wb')
                    await f.write(await resp.read())
                    await f.close()
    
    # Delete cached images that are no longer in top 20
    print("Deleting old images ...")
    for img in cached_img:
        if img.rsplit(".", 1)[0] in old_imgs:
            os.remove(os.path.join(img_cache_dir, img))

# Connects to database, and cache images
async def connect_database_and_cache_images():
    engine = sqlalchemy_connect()
    top_memes_data = get_top_memes_data_from_db(engine)
    await cache_img(top_memes_data)
    return engine, top_memes_data

# Check if PDF file needs to be regenerated
# Allows PDF file that are generated "seconds" ago be immediately used again (avoid slow reply if user spams "/generate")
def regeneration_check(seconds):
    print("Checking if file needs to be regenerated")
    reports_dir = os.path.join(os.path.dirname(os.path.dirname(__file__)), "reports")
    html_report_path = os.path.join(reports_dir, "report.html")
    pdf_report_path = os.path.join(reports_dir, "report.pdf")
    
    if os.path.exists(html_report_path):
        modified_seconds_ago = (datetime.now() - datetime.fromtimestamp(os.path.getmtime(html_report_path))).total_seconds()
        if os.path.exists(pdf_report_path) and modified_seconds_ago < seconds:
            return pdf_report_path
    
    return None


## Preparing the table in report
# Utility function to get cached image path from name of post
def get_full_img_path(df):
    name = f"{df.iloc[0]}.{df.iloc[1].rsplit(".", 1)[-1].split("?", 1)[0]}"
    img_cache_dir = os.path.join(os.path.dirname(os.path.dirname(__file__)), "img_cache")
    full_path = os.path.join(img_cache_dir, name)
    if os.path.exists(full_path):
        return full_path
    elif df.iloc[1] == "nsfw":
        return os.path.join(img_cache_dir, "nsfw.jpg")
    else:
        return os.path.join(img_cache_dir, "default.jpg")

# Utility function to get PIL image from cached image path
def get_thumbnail(path):
    img = Image.open(path)
    size = (128, 128)
    img.thumbnail(size, Image.Resampling.LANCZOS)
    return img

# Utility function to format emojis in strings into HTML recognisable unicode
def format_emoji(string):
    return pyemoji.entities(string)

# Utillity function to encode PIL image in base64 and returns as its HTML image component
def image_formatter(img):
    with BytesIO() as buffer:
        img.save(buffer, 'jpeg')
        b64_encoded_img = base64.b64encode(buffer.getvalue()).decode()
        return f'<img src="data:image/jpeg;base64,{b64_encoded_img}">'
    
# Utility function to return url string as HTML links
def url_formatter(string):
    return f'<a href="{string}">{string}</a>'

# Takes dataframe of top 20 memes, and prepare them for HTML generation
def get_df_for_display(old_df):
    df = old_df.copy()
    
    # Sort table by net votes = upvotes - downvotes
    df["net votes"] = df["upvotes"] - df["downvotes"]
    df = df.sort_values("net votes", ascending=False, ignore_index=True)

    # Format title with emojis
    df["title"] = df.title.map(lambda title: format_emoji(title))

    # Format url into HTML links
    df["url"] = df.url.map(lambda url: url_formatter(url))

    # Puts PIL images of thumbnails into dataframe
    df["img_path"] = df[["name", "thumbnail_url"]].apply(get_full_img_path, axis=1)
    df["img"] = df.img_path.map(lambda path: get_thumbnail(path))

    # Selects dataframe columns that shall be displayed in report
    display_df = df[["title", "img", "net votes", "url"]]
    display_df.rename(columns={
        'title': 'Title', 
        'img': 'Thumbnail',
        'net votes': 'Net Votes',
        'url': 'Link'
    }, inplace=True)

    # Index starts with 1
    display_df.index += 1

    # Convert dataframe to HTML table
    return display_df.to_html(formatters={'Thumbnail': image_formatter}, escape=False).replace('<th>', '<th align="center">').replace("’", "'")


## Preparing the graph in report
# Takes dataframe with top 20 meme's upvote and downvote histories, and plot a votes against time graph using seaborn
def plot_time_series_graph(df):
    df["net votes"] = df["upvotes"] - df["downvotes"]

    # For graph legend order
    df_title_sorted_by_votes = df[df["crawled_at"] == df["crawled_at"].max()].sort_values("net votes", ascending=False).drop_duplicates(subset=["title"])["title"]
    
    # For better line colours
    palette = sb.color_palette(cc.glasbey, n_colors=20)
    
    # Plot the graph
    print("Plotting chart ...")
    lineplot = sb.lineplot(df, x="crawled_at", y="net votes", hue="title", hue_order=df_title_sorted_by_votes, palette=palette)
    
    # Format time axis ticks
    lineplot.xaxis.set_major_formatter(mdates.DateFormatter('%b %-d, %H:%M:%S'))
    
    # Format votes axis ticks (50000 to 50k)
    ylabels = [f'{x}k' for x in lineplot.get_yticks()/1000]
    lineplot.tick_params(axis='x', labelrotation=45)
    lineplot.set_yticklabels(ylabels)

    # Move graph legend to right of graph
    lineplot.legend(title='Meme Title', loc='center left', bbox_to_anchor=(1.05, 0.5), ncol=1)

    # Save graph as image file
    fig = lineplot.get_figure()
    img_cache_dir = os.path.join(os.path.dirname(os.path.dirname(__file__)), "img_cache")
    chart_path = os.path.join(img_cache_dir, "chart.png")
    print("Saving chart ...")
    fig.savefig(chart_path, dpi=300, bbox_inches = "tight")
    print("Saved")

    # Important! Close plot so consecutive plots do not overlap on each other
    plt.clf()
    plt.close()

# Pulls data from database and plot the graph based on data
def fetch_data_and_plot_graph(engine):
    time_series_data = get_upvote_time_series_of_top_memes(engine)
    plot_time_series_graph(time_series_data)

# Converts graph image path into HTML image component
def get_chart_html():
    img_cache_dir = os.path.join(os.path.dirname(os.path.dirname(__file__)), "img_cache")
    chart_path = os.path.join(img_cache_dir, "chart.png")
    return f'<img class="chart" src="{chart_path}">'


## Generates report in HTML format using jinja2
def generate_html_report(top_memes_data, timestamp):
    table_html = get_df_for_display(top_memes_data)
    chart_html = get_chart_html()
    
    templates_dir = os.path.join(os.path.dirname(os.path.dirname(__file__)), "templates")
    env = Environment(loader=FileSystemLoader(templates_dir))
    template = env.get_template("report_template.html")

    kwargs = {
        "page_title_text" : "Meme Report",
        "title_text" : "Top 20 memes of r/memes in the past 24 hours",
        "chart": chart_html,
        "time_generated_text" : f"This report is generated at {timestamp}",
        "table_title_text" : "Top 20 memes",
        "meme_table" : table_html
    }

    print("Generating html report...")
    html = template.render(**kwargs)

    reports_dir = os.path.join(os.path.dirname(os.path.dirname(__file__)), "reports")
    if not os.path.exists(reports_dir):
        os.mkdir(reports_dir)
    html_report_path = os.path.join(reports_dir, "report.html")
    with open(html_report_path, 'w') as f:
        f.write(html)
    return html_report_path


## Converts HTML report to PDF report using weasyprint
def generate_pdf_report(html_report_path):
    reports_dir = os.path.join(os.path.dirname(os.path.dirname(__file__)), "reports")
    pdf_report_path = os.path.join(reports_dir, "report.pdf")
    css = CSS(string='''
        @page {size: A4; margin: 1cm;} 
        .chart {width: 100%;}
        th {text-align: center; border: 1px solid black;}
        td {border: 1px solid black;}
        ''')
    print("Generating pdf report ...")
    HTML(html_report_path).write_pdf(pdf_report_path, stylesheets=[css])
    print("Finished generation")
    return pdf_report_path


# Main function to fetch data, cache data, prepares table, plot graph, generate HTML and PDF reports
async def main():
    pdf_report_path = regeneration_check(REGENERATE_AFTER_SECONDS)
    if pdf_report_path is None:
        timestamp = await get_newest_update()
        engine, top_memes_data = await connect_database_and_cache_images()
        fetch_data_and_plot_graph(engine)
        html_report_path = generate_html_report(top_memes_data, timestamp)
        pdf_report_path = generate_pdf_report(html_report_path)
        engine.dispose()
    return pdf_report_path


if __name__ == '__main__':
    asyncio.run(main())

