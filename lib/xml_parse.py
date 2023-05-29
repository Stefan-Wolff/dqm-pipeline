# support xml parsing
import xml.sax
from collections import deque


class XMLHandler(xml.sax.ContentHandler):

	def __init__(self, searchFor):		
		self.searchFor = searchFor
		
		# order by group paths (second structure for easy access at XMLHandler#endElement())
		self.groupPaths = {}
		for elementPath, group in self.searchFor.items():
			if "groupPath" in group:
				group["elementPath"] = elementPath
				self.groupPaths.setdefault(group["groupPath"], [])
				self.groupPaths[group["groupPath"]].append(group)
				
	
		self.curPath = ""
		self.curValue = ""
		self.pathStack = deque([""])
		self.data = {}


	def startElement(self, tag, attributes):
		self.curPath = self.curPath + "/" + tag
		self.pathStack.append(self.curPath)
		self.reading = self.curPath in self.searchFor
		
		
	def endElement(self, tag):
		# save last read value
		if self.reading:
			if self.curValue:
				# manage single and multi values
				if not self.curPath in self.data:
					self.data[self.curPath] = self.curValue
				else:
					if not isinstance(self.data[self.curPath], list):
						self.data[self.curPath] = [self.data[self.curPath]]

					self.data[self.curPath].append(self.curValue)
				
				self.curValue = ""
			self.reading = False
			
			
		# process group members
		if self.curPath in self.groupPaths:
			group_entry = {}
			for group in self.groupPaths[self.curPath]:
				if not group["groupName"]:
					continue
				
				if group["elementPath"] in self.data:
					if group["elementName"]:
						group_entry[group["elementName"]] = self.data[group["elementPath"]]
					del self.data[group["elementPath"]]
			
			if group_entry:
				self.data.setdefault(group["groupName"], [])
				self.data[group["groupName"]].append(group_entry)
		
		
		self.pathStack.pop()
		self.curPath = self.pathStack[-1]
		
		
	def characters(self, content):
		if self.reading:
			self.curValue += content
			
			
	def finalize(self):
		result = {}
		
		# simple values
		for elementPath, group in self.searchFor.items():
			if not "groupName" in group and elementPath in self.data and group["elementName"]:
				result[group["elementName"]] = self.data[elementPath]
		
		# groups
		for groups in self.groupPaths.values():
			groupName = groups[0]["groupName"]
			if groupName in self.data:
				result[groupName] = self.data[groupName]
		
		return result



class Parser:
	def __init__(self):
		self.parser = xml.sax.make_parser()
		self.parser.setFeature(xml.sax.handler.feature_namespaces, 0)

	def parse(self, handler, file):
		self.parser.setContentHandler(handler)
		self.parser.parse(file)
	
		return handler.finalize()

