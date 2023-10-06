import pandas as pd
import streamlit as st
import pymysql
import pymongo
from pymongo import MongoClient
import googleapiclient.discovery
from googleapiclient.discovery import build
import plotly.express as px

# BUILDING CONNECTION WITH YOUTUBE API
api_key= "AIzaSyAskL7NKXofZkzpr24JDNgr9PfVpzaeexE"
api_service_name = "youtube"
api_version = "v3"
DEVELOPER_KEY = "AIzaSyAskL7NKXofZkzpr24JDNgr9PfVpzaeexE"

youtube = googleapiclient.discovery.build(
        api_service_name, api_version, developerKey = DEVELOPER_KEY)

# Bridging a connection with MongoDB-existing database(project)
client = pymongo.MongoClient('mongodb://localhost:27017')  
mydb = client["project"]
information = mydb.youtube

# Pushing data from mongodb to python
mongo_client = MongoClient('mongodb://localhost:27017')
db = mongo_client['project']
information1 = db.youtube


# Bridging a connection with Mysql Database
myconnection = pymysql.connect(host = '127.0.0.1',user='root',passwd='Hari@161297')
cur = myconnection.cursor()
myconnection = pymysql.connect(host = '127.0.0.1',user='root',passwd='Hari@161297',database = "youtube")
cur = myconnection.cursor()

st.title("YouTube Data Harvesting and Warehousing")

# FUNCTION TO GET CHANNEL DETAILS
def youtube_data(channel_id):
    final_data=[]
    request = youtube.channels().list(
        part="snippet,contentDetails,statistics ",
        id=channel_id
    )
    response = request.execute()
    
    for i in range(len(response["items"]):
        finalinfo=dict(channel_name=response["items"][i]["snippet"]["title"],
                       channel_id= response["items"][i]["id"],
                       channel_description=response["items"][i]["snippet"]["description"],
                       subcription_count= response["items"][i]["statistics"]["subscriberCount"],
                       video_count= response["items"][i]["statistics"]["videoCount"],
                       channel_views= response["items"][i]["statistics"]["viewCount"],
                       uploads_id = response['items'][i]['contentDetails']['relatedPlaylists']['uploads'])
        final_data.append(finalinfo)

    return final_data

# FUNCTION TO GET PLAYLIST DETAILS
def youtube_playlist_data(channel_id):    
    playlist_data=[]
    
    youtube = googleapiclient.discovery.build("youtube", "v3", developerKey = "AIzaSyAskL7NKXofZkzpr24JDNgr9PfVpzaeexE")

    request = youtube.playlists().list(
        part="snippet,contentDetails",
        channelId=channel_id,
        maxResults=50
    )
    response = request.execute()



    for i in range(len(response["items"])):
        playlistinfo=dict(channel_id= response["items"][i]['snippet']['channelId'],
                          playlist_id = response["items"][i]['id'],
                          playlist_name = response["items"][i]['snippet']['title']
                         )
        playlist_data.append(playlistinfo)

    return playlist_data

# FUNCTION TO GET VIDEO IDS
def get_all_video_ids(channel_id):
    video_ids = []

    # Fetch the channel's content details, including the uploads playlist
    channel_info = youtube.channels().list(part='contentDetails', id=channel_id).execute()
    uploads_playlist_id = channel_info['items'][0]['contentDetails']['relatedPlaylists']['uploads']

    # Retrieve all videos from the uploads playlist
    videos_request = youtube.playlistItems().list(
        part='contentDetails',
        maxResults=50,  
        playlistId=uploads_playlist_id
    )

    while videos_request:
        videos_response = videos_request.execute()

        for item in videos_response.get('items', []):
            video_id = item['contentDetails']['videoId']
            video_ids.append(video_id)

        # Fetch the next page of videos (if available)
        videos_request = youtube.playlistItems().list_next(videos_request, videos_response)

    return video_ids


# Function to convert duration strings to HH:MM:SS format
def convert_duration(duration_str):
    # Remove the "PT" prefix if present
    duration_str = duration_str.replace("PT", "")
    
    # Initialize hours, minutes, and seconds to 0
    hours, minutes, seconds = 0, 0, 0
    
    # Parse the duration string to extract hours, minutes, and seconds
    if 'H' in duration_str:
        hours_str, duration_str = duration_str.split('H')
        hours = int(hours_str)
    if 'M' in duration_str:
        minutes_str, duration_str = duration_str.split('M')
        minutes = int(minutes_str)
    if 'S' in duration_str:
        seconds_str = duration_str.replace('S', '')
        seconds = int(seconds_str)
    
    # Format the duration as HH:MM:SS
    formatted_duration = f"{hours:02}:{minutes:02}:{seconds:02}"
    
    return formatted_duration


# FUNCTION TO GET VIDEO DETAILS
def videodetails(all_video_ids):
    videodetails=[]
    for i in range(0,len(all_video_ids),50):
        request = youtube.videos().list(
        part="snippet,statistics,contentDetails",
        id=",".join(all_video_ids[i:i+50])
    )
        response = request.execute()
        
        for i in range(len(response["items"])):
            videodata=dict(#playlist_id=p_id,
                           channel_id = response['items'][i]['snippet']['channelId'],
                           video_id = response["items"][i]["id"], 
                           video_name=response["items"][i]["snippet"]["title"],
                           video_description= response["items"][i]["snippet"]["description"],
                           published_date= response["items"][i]["snippet"]["publishedAt"],  
                           view_count= response["items"][i]["statistics"]["viewCount"],
                           comment_count= response["items"][i]["statistics"]["commentCount"],
                           like_count= response["items"][i]["statistics"]["likeCount"],
                           duration= convert_duration(response["items"][i]["contentDetails"]["duration"]))
            
            videodetails.append(videodata)

    return videodetails

# FUNCTION TO GET COMMENT DETAILS
def commentdetails(ids):
    commentdetails=[]
    for j in ids:
        try:
            request = youtube.commentThreads().list(
            part="snippet,replies",
            videoId = j,
            maxResults = 100)
            response = request.execute()

            for i in range(len(response["items"])):
                commentdata=dict(comment_id = response["items"][i]['snippet']['topLevelComment']["id"],
                               video_id = response["items"][i]['snippet']['topLevelComment']['snippet']['videoId'],
                               comment_text = response["items"][i]['snippet']['topLevelComment']['snippet']['textOriginal'],
                               comment_author = response["items"][i]['snippet']['topLevelComment']['snippet']['authorDisplayName'],
                               comment_date = response["items"][i]['snippet']['topLevelComment']['snippet']['publishedAt'])

                commentdetails.append(commentdata)

        except:
            pass
    return commentdetails

#Main function to get all the details
def main(channel_id):
    a = youtube_data(channel_id)
    b = youtube_playlist_data(channel_id)
    c = get_all_video_ids(channel_id)
    d = videodetails(c)
    e = commentdetails(c)
    
    final_youtube_data={"channel details" : a,
                        "playlist details" : b,
                        "video details" : d,
                        "comment details" : e}
    
    return final_youtube_data

c_id = st.text_input("Enter your channel id:")

a=st.button("Extract data and store in MongoDB")

# FUNCTION TO GET CHANNEL NAMES FROM MONGODB
  
ch_ids = []
for i in information1.find({},{"_id":0,"channel details":1}):
     ch_ids.append(i["channel details"][0]['channel_id'])
 

if c_id not in ch_ids:
    if a and c_id:
        data = main(c_id)
        information.insert_one(data)
        st.success("successfully Uploaded to MongoDB !!", icon="✅")
else:
     st.write("## :red[Already Exists]")


# FUNCTION TO GET CHANNEL NAMES FROM MONGODB
def channel_names():   
    ch_name = []
    for i in information1.find({},{"_id":0,"channel details":1}):
        ch_name.append(i["channel details"][0]['channel_name'])
    return ch_name



s=st.selectbox("Select channel",options= channel_names())
b=st.button("Migrate data from MongoDB to MySQL:")

#GET ALL DETAILS FROM MONGODB        
data1=[]
for i in information1.find({},{"_id":0,"channel details":1,"playlist details":1,"video details":1,"comment details":1}):
    if i['channel details'][0]['channel_name']==s:
      data1.append(i)

df1= pd.DataFrame(data1[0]["channel details"])
df2= pd.DataFrame(data1[0]["playlist details"])
df3= pd.DataFrame(data1[0]["video details"])
df3["duration"] = pd.to_datetime(df3["duration"])
df3["published_date"] = pd.to_datetime(df3["published_date"])
df4= pd.DataFrame(data1[0]["comment details"])
df4["comment_date"] = pd.to_datetime(df4["comment_date"])


cur.execute("select channel_name from channel;")
c = [i[0] for i in cur.fetchall()]


if s not in c:
    if b:
        sql1 = "insert into channel (channel_name,channel_id,channel_description,subcription_count,video_count,channel_views,uploads_id) values (%s,%s,%s,%s,%s,%s,%s)"
        for i in range(0,len(df1)):
                cur.execute(sql1,tuple(df1.iloc[i]))
                myconnection.commit()

        sql2 = "insert into playlist (channel_id,playlist_id,playlist_name) values (%s,%s,%s)"
        for i in range(0,len(df2)):
            cur.execute(sql2,tuple(df2.iloc[i]))
            myconnection.commit()

        sql3 = "insert into video (channel_id,video_id,video_name,video_description,published_date,view_count,comment_count,like_count,duration) values (%s,%s,%s,%s,%s,%s,%s,%s,%s)"
        for i in range(0,len(df3)):
            cur.execute(sql3,tuple(df3.iloc[i]))
            myconnection.commit()
            
        sql4 = "insert into comment (comment_id,video_id,comment_text,comment_author,comment_date) values (%s,%s,%s,%s,%s)"
        for i in range(0,len(df4)):
            cur.execute(sql4,tuple(df4.iloc[i]))
            myconnection.commit()
            
        st.success("Successfully migrated to SQL !!", icon="✅")
else:
     st.write("## :red[Already Exists]")
    
            

st.write("## :green[Select any question to get Insights]")
questions = st.selectbox('Questions',
   ['1. What are the names of all the videos and their corresponding channels?',
    '2. Which channels have the most number of videos, and how many videos do they have?',
    '3. What are the top 10 most viewed videos and their respective channels?',
    '4. How many comments were made on each video, and what are their corresponding video names?',
    '5. Which videos have the highest number of likes, and what are their corresponding channel names?',
    '6. What is the total number of likes and dislikes for each video, and what are their corresponding video names?',
    '7. What is the total number of views for each channel, and what are their corresponding channel names?',
    '8. What are the names of all the channels that have published videos in the year 2022?',
    '9. What is the average duration of all videos in each channel, and what are their corresponding channel names?',
    '10. Which videos have the highest number of comments, and what are their corresponding channel names?']) 
      
if questions == '1. What are the names of all the videos and their corresponding channels?':
    cur.execute("select b.video_name,a.channel_name from channel as a inner join video as b on a.channel_id=b.channel_id")
    df = pd.DataFrame(cur.fetchall())
    st.write(df)   

elif questions == '2. Which channels have the most number of videos, and how many videos do they have?':
        cur.execute("select distinct channel_name,video_count from channel order by video_count desc")
        df = pd.DataFrame(cur.fetchall())
        st.write(df)
        st.write("### :green[Number of videos in each channel :]")      
        fig = px.bar(df,x=0,y=1,orientation='v',color=0)
        st.plotly_chart(fig,use_container_width=True)

elif questions == '3. What are the top 10 most viewed videos and their respective channels?':
        cur.execute("select b.video_name,b.view_count,a.channel_name from channel as a inner join video as b on a.channel_id=b.channel_id order by view_count desc limit 10")
        df = pd.DataFrame(cur.fetchall())
        st.write(df)
        st.write("### :green[Top 10 most viewed videos :]")      
        fig = px.bar(df,x=0,y=1,orientation='v',color=0)
        st.plotly_chart(fig,use_container_width=True)

elif questions == '4. How many comments were made on each video, and what are their corresponding video names?':
        cur.execute("select video_name,comment_count from video")
        df = pd.DataFrame(cur.fetchall())
        st.write(df)

elif questions == '5. Which videos have the highest number of likes, and what are their corresponding channel names?':
        cur.execute("select a.channel_name,b.video_name,b.like_count from channel as a inner join video as b on a.channel_id=b.channel_id order by like_count desc limit 10;")
        df = pd.DataFrame(cur.fetchall())
        st.write(df)
        st.write("### :green[Top 10 most liked videos :]")
        fig = px.bar(df,x=2,y=1,orientation='h',color=0)
        st.plotly_chart(fig,use_container_width=True)

elif questions == '6. What is the total number of likes and dislikes for each video, and what are their corresponding video names?':
        cur.execute("select video_name,like_count from video")
        df = pd.DataFrame(cur.fetchall())
        st.write(df)

elif questions == '7. What is the total number of views for each channel, and what are their corresponding channel names?':
        cur.execute("select channel_name,channel_views from channel;")
        df = pd.DataFrame(cur.fetchall())
        st.write(df)
        st.write("### :green[Channels vs Views :]")
        fig = px.bar(df,x=0,y=1,orientation='v',color=0)
        st.plotly_chart(fig,use_container_width=True)

elif questions == '8. What are the names of all the channels that have published videos in the year 2022?':
        cur.execute("select distinct a.channel_name from channel as a inner join video as b on a.channel_id=b.channel_id where published_date LIKE '2022%' ")
        df = pd.DataFrame(cur.fetchall())
        st.write(df)

elif questions == '9. What is the average duration of all videos in each channel, and what are their corresponding channel names?':
        cur.execute("SELECT a.channel_name,time_format(SEC_TO_TIME(AVG(TIME_TO_SEC(b.duration))),'%H:%i:%s') AS duration FROM channel a inner join video b where b.channel_id=a.channel_id group by a.channel_name")
        df = pd.DataFrame(cur.fetchall())
        #df[1]=pd.to_datetime(df[1],format="%H$M%S.%f").datetime.hour
        st.write(df)
        st.write("### :green[Avg video duration for channels :]")
        fig = px.bar(df,x=0,y=1,orientation='v',color=0)
        st.plotly_chart(fig,use_container_width=True)

elif questions == '10. Which videos have the highest number of comments, and what are their corresponding channel names?':
        cur.execute("select a.channel_name,b.video_name,b.comment_count from channel as a inner join video as b on a.channel_id=b.channel_id order by comment_count desc limit 10")
        df = pd.DataFrame(cur.fetchall())
        st.write(df)
        st.write("### :green[Videos with most comments :]")
        fig = px.bar(df,x=1,y=2,orientation='v',color=0)
        st.plotly_chart(fig,use_container_width=True)       
