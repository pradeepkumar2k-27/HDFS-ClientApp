import os

os.system("tput setaf 2")
print("\n\n\t\t\t\tWelcome to Big Data HDFS Storage Cluster")
print("\t\t\t\t----------------------------------------\n\n")

def uploadFile():
	while(1):
		os.system("tput setaf 5")
		print("Local Storage Files : ")
		print("-------------------")
		os.system("ls -lh ./Uploads/")
		print("")
		file_name = input("Enter file name which you want to upload (press q to exit) ? ")
		if file_name == "q":
			print("")
			return
		os.system("hadoop fs -Ddfs.block.size=614400 -put ./Uploads/" + file_name + " /pythonUser/")
		print("")

def listFile():
	os.system("tput setaf 5")
	print("HDFS Files : ")
	print("------------")
	os.system("hadoop fs -ls /pythonUser")
	print("")

def downloadFile():
	while(1):
		listFile()
		file_name = input("Enter file name which you want to download (press q to exit) ? ")
		if file_name == "q":
			print("")
			return
		os.system("hadoop fs -get /pythonUser/" + file_name + " ./Downloads/" + file_name)
		print("")

def readFile():
	while(1):
		listFile()
		file_name = input("Enter file name which you want to read (press q to exit) ? ")
		print("")
		if file_name == "q":
			print("")
			return
		os.system("tput setaf 3")
		print(file_name)
		print("-----------")
		os.system("hadoop fs -cat /pythonUser/" + file_name)
		print("")

def deleteFile():
	while(1):
		listFile()
		file_name = input("Enter file name which you want to delete (press q to exit) ? ")
		if file_name == "q":
			print("")
			return
		os.system("hadoop fs -rm /pythonUser/" + file_name)
		print("")

while(1):
	os.system("tput setaf 6")
	print("Opertations :")
	print("-------------")
	print("""1. Hadoop HDFS Report\n2. List Files from Hadoop Distributed File Storage\n3. Upload File\n4. Download File\n5. Read File\n6. Delete File\n7. Exit Application\n
	""")

	os.system("tput setaf 7")
	ch = int(input("Select Operation : "))
	print("")
	if ch == 1:
		os.system("tput setaf 3")
		print("HDFS Report :")
		print("-----------")
		os.system("hadoop dfsadmin -report")
	elif ch == 2:
		listFile()
	elif ch == 3:
		uploadFile()
	elif ch == 4:
		downloadFile()
	elif ch == 5:
		readFile()
	elif ch == 6:
		deleteFile()
	elif ch == 7:
		os.system("tput setaf 7")
		print("Closing Application!!!\n\n")
		exit()
	else :
		os.system("tput setaf 1")
		print("Invalid Operation")
