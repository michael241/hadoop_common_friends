#imports
from mrjob.job import MRJob
from mrjob.step import MRStep
import re

class MR_program(MRJob):	

	def mapper_1 (self, _,line):
		#cleans line input
		textPurger = re.compile('[^a-zA-Z,:]')
		line = textPurger.sub('', line)
		
		#splits into person with friend and their friends
		alpha,beta = line.split(":")
		beta =list(beta)
		
		#yield list each pairing (key) and the person associated with it (current iteration only accepts 3 or 4 friends in the input)
		if len(beta)==3:
			yield [beta[0],beta[1]],alpha
			yield [beta[1],beta[2]],alpha
			yield [beta[2],beta[0]],alpha
		if len(beta)==4:
			yield [beta[0],beta[1]],alpha
			yield [beta[1],beta[2]],alpha
			yield [beta[2],beta[3]],alpha
			yield [beta[3],beta[0]],alpha
			yield [beta[0],beta[2]],alpha
			yield [beta[1],beta[3]],alpha
			
	def mapper_2(self, key, value):
		#rerranges keys so they are alphabetical, all pairs have same value despite some originally being inverse
		key = sorted(key)
		yield key, value
		
	def reducer_1(self, key,value):
		#reduce by key, append shared friends together for each pair
		yield key, value

	def steps(self):
		#orders the map reduce framework
		return [
			MRStep(mapper=self.mapper_1),
			MRStep(mapper=self.mapper_2),
			MRStep(reducer=self.reducer_1)
			]
	
		
if __name__ == '__main__':
	#executes mrjob
	MR_program.run()

#execute with: % python common_friends.py friends.txt -q 
