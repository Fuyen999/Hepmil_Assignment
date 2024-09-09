import pandas as pd
import seaborn as sb
from crawler import *
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

pd.options.mode.chained_assignment = None 

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


async def cache_img(df):
    img_cache_dir = os.path.join(os.path.dirname(os.path.dirname(__file__)), "img_cache")
    if not os.path.exists(img_cache_dir):
        os.mkdir(img_cache_dir)
    
    cached_img = os.listdir(img_cache_dir)
    cached_img_name = [name.rsplit(".", 1)[0] for name in cached_img]
    uncached_img_df = df[~df["name"].isin(cached_img_name)]
    old_imgs = list(set(cached_img_name) - set(df.name) - set(["nsfw", "default", "chart"]))

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
    
    print("Deleting old images ...")
    for img in cached_img:
        if img.rsplit(".", 1)[0] in old_imgs:
            os.remove(os.path.join(img_cache_dir, img))


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


def get_thumbnail(path):
    img = Image.open(path)
    size = (128, 128)
    img.thumbnail(size, Image.Resampling.LANCZOS)
    return img


def format_emoji(string):
    return pyemoji.entities(string)


def image_formatter(img):
    with BytesIO() as buffer:
        img.save(buffer, 'jpeg')
        b64_encoded_img = base64.b64encode(buffer.getvalue()).decode()
        return f'<img src="data:image/jpeg;base64,{b64_encoded_img}">'
    

def url_formatter(string):
    return f'<a href="{string}">{string}</a>'


def get_df_for_display(old_df):
    df = old_df.copy()
    df["net votes"] = df["upvotes"] - df["downvotes"]
    df["title"] = df.title.map(lambda title: format_emoji(title))
    df["url"] = df.url.map(lambda url: url_formatter(url))
    df["img_path"] = df[["name", "thumbnail_url"]].apply(get_full_img_path, axis=1)
    df["img"] = df.img_path.map(lambda path: get_thumbnail(path))
    display_df = df[["title", "img", "net votes", "url"]]
    display_df.rename(columns={
        'title': 'Title', 
        'img': 'Thumbnail',
        'net votes': 'Net Votes',
        'url': 'Link'
    }, inplace=True)
    display_df.index += 1
    return display_df.to_html(formatters={'Thumbnail': image_formatter}, escape=False).replace('<th>', '<th align="center">').replace("’", "'")


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


def plot_time_series_graph(df):
    df["net votes"] = df["upvotes"] - df["downvotes"]
    df_title_sorted_by_votes = df[df["crawled_at"] == df["crawled_at"].max()].sort_values("net votes", ascending=False)["title"]
    palette = sb.color_palette(cc.glasbey, n_colors=20)
    lineplot = sb.lineplot(df, x="crawled_at", y="net votes", hue="title", hue_order=df_title_sorted_by_votes, palette=palette)
    xlabels = df["crawled_at"].dt.strftime("%b %-d, %-I%p")
    ylabels = [f'{x}k' for x in lineplot.get_yticks()/1000]
    lineplot.set_xticklabels(xlabels)
    lineplot.tick_params(axis='x', labelrotation=45)
    lineplot.set_yticklabels(ylabels)
    lineplot.legend(title='Meme Title', loc='center left', bbox_to_anchor=(1.05, 0.5), ncol=1)

    fig = lineplot.get_figure()
    img_cache_dir = os.path.join(os.path.dirname(os.path.dirname(__file__)), "img_cache")
    chart_path = os.path.join(img_cache_dir, "chart.png")
    fig.savefig(chart_path, dpi=300, bbox_inches = "tight")


def get_chart_html():
    img_cache_dir = os.path.join(os.path.dirname(os.path.dirname(__file__)), "img_cache")
    chart_path = os.path.join(img_cache_dir, "chart.png")
    return f'<img class="chart" src="{chart_path}">'


async def generate_html_report():
    await newest_update()
    engine = sqlalchemy_connect()
    top_memes_data = get_top_memes_data_from_db(engine)
    await cache_img(top_memes_data)
    time_series_data = get_upvote_time_series_of_top_memes(engine)
    
    plot_time_series_graph(time_series_data)
    table_html = get_df_for_display(top_memes_data)
    chart_html = get_chart_html()
    
    templates_dir = os.path.join(os.path.dirname(os.path.dirname(__file__)), "templates")
    env = Environment(loader=FileSystemLoader(templates_dir))
    template = env.get_template("report_template.html")

    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    kwargs = {
        "page_title_text" : "Meme Report",
        "title_text" : "Top 20 memes of r/memes in the past 24 hours",
        "chart": chart_html,
        "time_generated_text" : f"This report is generated at {timestamp}",
        "table_title_text" : "Top 20 memes",
        "meme_table" : table_html
    }

    print("Generating report...")
    html = template.render(**kwargs)

    reports_dir = os.path.join(os.path.dirname(os.path.dirname(__file__)), "reports")
    if not os.path.exists(reports_dir):
        os.mkdir(reports_dir)
    html_report_path = os.path.join(reports_dir, "report.html")
    with open(html_report_path, 'w') as f:
        f.write(html)


async def generate_pdf_report():
    reports_dir = os.path.join(os.path.dirname(os.path.dirname(__file__)), "reports")
    html_report_path = os.path.join(reports_dir, "report.html")
    pdf_report_path = os.path.join(reports_dir, "report.pdf")
    modified_seconds_ago = (datetime.now() - datetime.fromtimestamp(os.path.getmtime(html_report_path))).total_seconds()
    
    if not os.path.exists(html_report_path) or modified_seconds_ago > 180:
        await generate_html_report()

        css = CSS(string='''
            @page {size: A4; margin: 1cm;} 
            .chart {width: 100%;}
            th {text-align: center; border: 1px solid black;}
            td {border: 1px solid black;}
            ''')
        HTML(html_report_path).write_pdf(pdf_report_path, stylesheets=[css])
    
    return pdf_report_path


if __name__ == '__main__':
    asyncio.run(generate_pdf_report())

