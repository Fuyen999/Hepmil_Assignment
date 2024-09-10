# Hepmil_Assignment
This is a repo for Hepmil's Software Engineer interview assignment

## Tasks
Kindly find the assignment over here:
1. Create a webservice that crawls [https://www.reddit.com/r/memes/](https://www.reddit.com/r/memes/) and returns top 20 voted posts for the past 24 hours. Sorted by top voted post first, descending order. 
2. Stores the crawled data into a database for historical tracking and future data visualization.
3. Present and generate a report file for past 24 hrs top 20 trending memes that can be sent as a file via a Telegram Chatbot.
4. Create a presentation deck to showcase live demo and explain both frontend and backend designs. 
5. Suggest 3 alternative use cases or actionable insights from the generated report. 

Level Marking Scheme:
- Level 1: Demonstrates ability to do data mining and converts the mining function into an API.
- Level 2: Demonstrates ability to create well structured database for crawled data.
- Level 3: Clear documentation, structure, design at code and service design for data visualisation.  
- Level 4: Able to provide explain mining methodologies and frameworks used.
- Level 5: Abe to showcase actionable insights after generating the report.

## Project Description
The project directories are shown below
```
Hepmil_Assignment
  ├── img_cache (stores images required in report)
  ├── reports (stored generated report)
  ├── scripts
  │     ├── crawler.py (fetch data from reddit API)
  │     ├── generator.py (generates HTML and PDF reports)
  │     └── telegram_bot.py (telegram bot implementation)
  ├── templates (stores HTML report template)
  ├── presentation deck.pptx
  └── requirements.txt
```


## Setting up
- First, set up a python environment for this project (using conda, virtualenv etc.)
- Install dependencies in the environment by:
```
pip install -r requirements.txt
```
- Install weasyprint on the machine, following [weasyprint documentation](https://doc.courtbouillon.org/weasyprint/stable/first_steps.html#installation)
- Run `telegram_bot.py`. I ran it on deployed machine as systemd service.