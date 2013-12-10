#! /usr/bin/python
# coding: utf-8

name_fb = '/OccupyWallSt'
id_fb = 184749301592842
time_interval = 300
start_interval = 1335830400
end_interval = start_interval + time_interval
max_interval = 1335831000

import fbconsole
import pymongo
from pymongo import Connection
connection = Connection()
db = connection['facebook']
collection = db['wallposts']

fbconsole.ACCESS_TOKEN = ''
fbconsole.authenticate()

def getPosts():
		fb_data = fbconsole.fql("SELECT post_id, actor_id, message, message_tags, attachment, likes, comments, permalink, type, target_id, place, created_time FROM stream WHERE source_id = " + str(id_fb) + " and created_time > " + str(start_interval) + " AND created_time < " + str(end_interval))
		return fb_data

def printPosts():
		post = getPosts()
		print "from " + str(start_interval) + " to " + str(end_interval)
		count = len(post)
		for index in range(count):
				post_content = {}
				if post[index].has_key("post_id"):
						post_content['post_id'] = post[index]['post_id']
				else:
						post_content['post_id'] = 'none'

				if post[index].has_key("actor_id"):
						post_content['actor_id'] = post[index]['actor_id']
				else:
						post_content['actor_id'] = 'none'

				if post[index].has_key("message"):
						post_content['message'] = post[index]['message']
				else:
						post_content['message'] = 'none'

				if post[index].has_key("message_tags"):
						post_content['message_tags'] = post[index]['message_tags']
				else:
						post_content['message_tags'] = 'none'

				if post[index].has_key("likes"):
						if post[index]['likes'].has_key("count"):
								post_content['likes_count'] = post[index]['likes']['count']
						else:
								post_content['likes_count'] = 'none'
				else:
						post_content['likes_count'] = 'none'

				if post[index].has_key("comments"):
						post_content['comments_count'] = post[index]['comments']['count']
				else:
						post_content['comments_count'] = 'none'

				if post[index].has_key("permalink"):
						post_content['post_permalink'] = post[index]['permalink']
				else:
						post_content['post_permalink'] = 'none'

				if post[index].has_key("type"):
						post_content['post_type'] = post[index]['type']
				else:
						post_content['post_type'] = 'none'

				if post[index].has_key("target_id"):
						post_content['target_id'] = post[index]['target_id']
				else:
						post_content['target_id'] = 'none'

				if post[index].has_key("place"):
						post_content['place'] = post[index]['place']
				else:
						post_content['place'] = 'none'

				if post[index].has_key("created_time"):
						post_content['created_time'] = post[index]['created_time']
				else:
						post_content['created_time'] = 'none'

				collection.insert(post_content)

		return count

while end_interval < max_interval:
		postsQuantity = printPosts()
		print postsQuantity
		if postsQuantity >= 30:
				time_interval = 50
		elif (postsQuantity >= 20) & (postsQuantity < 30):
				time_interval = 150
		elif (postsQuantity >= 10) & (postsQuantity < 20):
				time_interval = 200
		else:
				time_interval = 300
		start_interval = end_interval
		end_interval = end_interval + time_interval
		if((max_interval - start_interval) < time_interval) & ((max_interval - start_interval) > 0):
				end_interval = start_interval + (max_interval - start_interval)
				print printPosts()