import urllib
import json
import datetime



def create_post_url(graph_url, APP_ID, APP_SECRET):
    #create authenticated post URL
    post_args = "/posts/?key=value&access_token=" + APP_ID + "|" + APP_SECRET
    post_url = graph_url + post_args

    return post_url

def render_to_json(graph_url):
    #render graph url call to JSON
    web_response = urllib.urlopen(graph_url)
    readable_page = web_response.read()
    json_data = json.loads(readable_page)

    return json_data

def scrape_posts_by_date(graph_url, date, post_data, APP_ID, APP_SECRET):
    #render URL to JSON
    page_posts = render_to_json(graph_url)

    #extract next page
    next_page = page_posts["paging"]["next"]

    #grab all posts
    page_posts = page_posts["data"]

    #boolean to tell us when to stop collecting
    collecting = True

    #for each post capture data
    for post in page_posts:
        try:
            likes_count = get_likes_count(post["id"], APP_ID, APP_SECRET)
            current_post = [post["id"], post["message"], likes_count,
                            post["created_time"], post["shares"]["count"]]

        except Exception:
            current_post = [ "error", "error", "error", "error"]

        if current_post[3] != "error":
            print (date)
            print (current_post[3])
            if date <= current_post[3]:
                post_data.append(current_post)

            elif date > current_post[3]:
                print ("Done collecting")
                collecting = False
                break


    #If we still don't meet date requirements, run on next page
    if collecting == True:
        scrape_posts_by_date(next_page, date, post_data, APP_ID, APP_SECRET)

    return post_data

def get_likes_count(post_id, APP_ID, APP_SECRET):
    #create Graph API Call
    graph_url = "https://graph.facebook.com/"
    likes_args = post_id + "/likes?summary=true&key=value&access_token" + APP_ID + "|" + APP_SECRET
    likes_url = graph_url + likes_args
    likes_json = render_to_json(likes_url)

    #pick out the likes count
    count_likes = likes_json["summary"]["total_count"]

    return count_likes

def main():
    #simple data pull App Secret and App ID
    APP_SECRET = "c3a6acca6125cdb9941c45aa7354cb47"
    APP_ID = "454029036027132"

    #to find go to page's FB page, at the end of URL find username
    #e.g. http://facebook.com/walmart, walmart is the username
    list_companies = ["walmart", "cisco", "pepsi", "facebook"]
    graph_url = "https://graph.facebook.com/"

    #the time of last weeks crawl
    last_crawl = datetime.datetime.now() - datetime.timedelta(weeks=1)
    last_crawl = last_crawl.isoformat()



    for company in list_companies:
        #make graph api url with company username
        current_page = graph_url + company

        #open public page in facebook graph api
        json_fbpage = render_to_json(current_page)


        #gather our page level JSON Data
        page_data = [json_fbpage["id"], json_fbpage["likes"],
                     json_fbpage["talking_about_count"],
                     json_fbpage["username"]]
        print (page_data)

        #extract post data
        post_url = create_post_url(current_page, APP_ID, APP_SECRET)

        post_data = []
        post_data = scrape_posts_by_date(post_url, last_crawl, post_data, APP_ID, APP_SECRET)



        print (post_data)



if __name__ == "__main__":
    main()