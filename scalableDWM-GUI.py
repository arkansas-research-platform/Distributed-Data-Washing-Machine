#!/usr/bin/env python
# coding: utf-8

import os
from tkinter import *
from tkinter import Tk
from tkinter import ttk
from tkmacosx import Button  # special library for macOS to change background colour in tKinter (only when using mac)
from tkinter import messagebox
import tarfile
import subprocess as sp
import threading
from tkinter import filedialog


# Basic Settings (Title, Icon, Window Size)
root = Tk()
root.title("SCALABLE DATA WASHING MACHINE")
root.iconphoto(False, PhotoImage(file="ualr2.png"))
root.geometry("1200x800")
root.configure(background='violet')

#===================== Variables ==================
# Define Functions
padding = dict(padx=50,pady=50)

filename='parmStage.txt'

#================= Define Functions ==================
# ---- Fxns used in Parameter Summary Widget 
def viewParms():
    # Open ParmStage File and Insert into TextBox if "view" button is pressed
    with open(filename, 'r') as f:
        psummText.delete("1.0", END)
        psummText.insert(INSERT, f.read())
        psummText.config(state=DISABLED)

def clearParms():
    psummText.config(state=NORMAL)
    psummText.delete("1.0", END)

# ---- Fxns used in Application & Cluster Setting Widget
def comboClick(event):
    clusCombLabel = Label(appFrame, text=clusCombo.get())

# ---- Fxns used in Execution Window Widget
def popup1():
    response = messagebox.askyesno(message="Proceed with job submission?") # returns 0 or 1
    # If the response is Yes, Execute the HDWM00 Driver Scripts
    if response:
        execText.delete("1.0", END)
        process = sp.Popen(["bash", "script.sh"], stdout=sp.PIPE, stderr=sp.PIPE, text=True)
        #result = process.communicate()
        #execText.insert(INSERT, result)
    while True:
        output = process.stdout.readline()
        if output:
            execText.insert(INSERT, output)
            execText.config(state=DISABLED)
        result = process.poll()
        if result is not None:
            break

# Note: This fxn (Threading) is used to call the Popup1 fxn. Without it, 
# the Submit popup will freeze until the Script.sh is done running
def start_popup1_thread(event):
    global Agenda_thread
    Agenda_thread = threading.Thread(target=popup1)
    Agenda_thread.daemon = True
    Agenda_thread.start()

def popup2():
    response = messagebox.askyesno(message="Exit appplication?") # returns 0 or 1
    #Label(root, text=response).pack()
    if response:
        root.destroy()

# ---- Fxns used in HDFS Directory Widget
def openWin():
    global image
    # Creating new window
    subWin1 = Toplevel(root)
    subWin1.title("Configure Cluster")
    subWin1.config(width=500, height=500)
    subWin1.grab_set()
    #label = Label(top, text="Hello World!").pack()
    #image = ImageTk.PhotoImage(Image.open("squirrel.jpeg"))
    #my_label = Label(top, image=image).pack()
    btn2 = Button(subWin1, text="Close Window", command=subWin1.destroy)#.grid(sticky='s')
    btn2.place(x=200, y=400)        


#===================== Main Frame to contains labelframes and Buttons =====================================
# Main Frame inside Root
mainframe = Frame(root,height=750, width=750)
#mainframe.grid(row=0,column=0,padx=20, pady=20,sticky="w")
mainframe.pack(fill="both", expand=True)

# Dynamycally resize the window
#Grid.rowconfigure(mainframe, 0, weight=1)
#Grid.columnconfigure(mainframe, 0, weight=1)
#Grid.rowconfigure(mainframe, 0, weight=1)
#Grid.columnconfigure(mainframe, 1, weight=2)
#Grid.rowconfigure(mainframe, 0, weight=1)
#Grid.columnconfigure(mainframe, 2, weight=2)
#Grid.rowconfigure(mainframe, 0, weight=1)
#Grid.columnconfigure(mainframe, 3, weight=2)
#Grid.rowconfigure(mainframe, 1, weight=1)
#Grid.columnconfigure(mainframe, 0, weight=2)

#=============== Parm Entry Box Label ===============
# Label 
entLabel = Label(mainframe, text="Enter a Valid Parameter File Name: ")
entLabel.grid(row=0, column=0, sticky=W, padx=30, pady=28)

# Entry Box for ParmFile
entInputBox = Entry(mainframe, width=38)
entInputBox.grid(row=0, column=0, columnspan=2, padx=50, pady=28, sticky=E)

#=============== Application Type & Cluster Selection Frame ===============
appType = StringVar()
appType.set(None)
clusVar = StringVar()
options = ["","Custom Cluster", "AR HPC Center"]
clusVar.set(0) # default value
# Parent Frame
appFrame = LabelFrame(mainframe,text='Application Settings', labelanchor="n", width=80)
appFrame.grid(row=1,column=0,padx=20, pady=20, sticky="nw")
appRadioButton1 = Radiobutton(appFrame, text="Hadoop Data Washing Machine", anchor='w', width=30, variable=appType, value="HDWM").grid(row=0, column=0, sticky='s')
appRadioButton2 = Radiobutton(appFrame, text="Spark Data Washing Machine", anchor='w', width=30, variable=appType, value="SDWM").grid(row=1, column=0, sticky='w')
clusLabel = Label(appFrame, text="Select Cluster Below")
clusLabel.grid(row=2, column=0, sticky=W, padx=5, pady=10)
#clusDD = OptionMenu(appFrame, clusVar, "Custom Cluster", "AR HPC Center").grid(row=2, column=1, sticky='w', padx=5, pady=10)
clusCombo = ttk.Combobox(appFrame, values=options, state='readonly')
clusCombo.current(None)
clusCombo.bind("<<ComboboxSelected>>", comboClick)
clusCombo.grid(row=3, column=0, sticky='w', padx=5)
configButton = Button(appFrame,text="Configure", relief="raised", justify="center", command=openWin).grid(row=4,column=0,sticky='s', padx=20)#, width=15).grid(row=4,column=0,sticky='s', padx=20) # or use ,**padding

#=============== HDFS Directory Frame ===============
# Parent Frame
hdfsDirFrame = LabelFrame(mainframe,text='HDFS Directory', labelanchor="n")
hdfsDirFrame.grid(row=2,column=0,padx=20, pady=0,sticky="w")

hdfsTextBox = Text(hdfsDirFrame, height=20, width=45, state=NORMAL)
hdfsTextBox.grid()

# Directly Opens root directory
#hdfsDirFrame.directory = filedialog.askdirectory()
#print (hdfsDirFrame.directory)

'''
foldersPath = "/Users/nick/Documents/Portfolio"
tree = ttk.Treeview()
tree.pack(fill='both', expand=True)

with open(foldersPath) as fo:
    lists = fo.getmembers()

for folder in lists:
    tree.insert('', 'end', folder, text=folder)
    for name in os.listdir(folder):
        tree.insert(folder, 'end', name, text=name)
'''
        
'''
# Create Tree-view of folders
tree = ttk.Treeview(root)
tree.pack()
tree.heading('#0', text="Item")
tree.column('#0', width=495)

# Get TAR items
with tarfile.TarFile("testing.tar") as topen:
    tarlist = topen.getmembers()


def insert():
    for item in tarlist:
        parent, label = os.path.split(item.path)
        tree.insert(parent, 'end', iid=item.path, text=label)

insert()
'''

# Open Directory Button
hdfsViewButton = Button(hdfsDirFrame,text="Open").grid(row=3, column=0, sticky=W)#, width=5, relief="raised").grid(row=3, column=0, sticky=W)
openLogButton = Button(hdfsDirFrame,text="LogFile").grid(row=3, column=0, sticky=S)#, width=5, relief="raised").grid(row=3, column=0, sticky=S)
openLnkIndButton = Button(hdfsDirFrame,text="Link Index").grid(row=3,column=0, sticky=E)#, width=5, relief="raised").grid(row=3,column=0, sticky=E)

#=============== Parameter Summary Frame ===============
# Parent Frame
psummFrame = LabelFrame(mainframe, text='Summary of Parameters', labelanchor="n")
psummFrame.grid(row=1, column=1, padx=5, pady=10, rowspan=2, sticky="w")

# Parameter Summary TextBox
#psummBox= Listbox(psummFrame, height=30, width=45, bg="white")
psummText = Text(psummFrame, height=38, width=45)
psummText.grid()
#psummText.grid_rowconfigure(1, weight=1)
#psummText.grid_columnconfigure(1, weight=1)
#psummText.grid(row=1,column=1, sticky='nsew')

# View Button
viewButton = Button(psummFrame, text="View Parms", relief="raised", command=viewParms)#,width=10, )
viewButton.grid(row=3,column=0, sticky='w', padx=15)

# Clear Button
clearButton = Button(psummFrame, text="Clear", relief="raised", command=clearParms)#, width=10)
clearButton.grid(row=3,column=0, sticky='e', padx=15)

#=============== Execution Window Frame ===============
# Parent Frame
execFrame = LabelFrame(mainframe,text='Execution', labelanchor="n")
execFrame.grid(row=1,column=2,padx=5, pady=10,rowspan=2, sticky="w")

# Execution TextBox
#execBox= Listbox(execFrame, height=30, width=60, bg="white")
execText = Text(execFrame, height=40, width=60, state=NORMAL)
execText.grid()
#execText.grid_rowconfigure(1, weight=1)
#execText.grid_columnconfigure(2, weight=1)
#execText.grid(row=1,column=2, sticky='nsew')


#=============== Current Status ===============
# Parent Frame
currentSettingFrame = LabelFrame(mainframe, relief="ridge")
currentSettingFrame.grid(row=4, column=0, padx=20, pady=10, sticky='w', columnspan=3) 

# Current MU
muLabel = Label(currentSettingFrame, text = "New Mu Value: ")
muLabel.grid(row=0, column=0, sticky=W, padx=5, pady=10)
muBox = Text(currentSettingFrame, height=2, width=5)
muBox.tag_configure("center",justify='center', font=("Comic Sans MS", 15))
muBox.tag_add("center",1.0,"end")
muBox.grid(row=0, column=1, padx=3)

# Current Epsilon
epsLabel = Label(currentSettingFrame, text = "New Epsilon Value: ")
epsLabel.grid(row=1, column=0, sticky=W, padx=5, pady=10)
epsBox = Text(currentSettingFrame, height=2, width=5)
epsBox.tag_configure("center",justify='center', font=("Comic Sans MS", 15))
epsBox.tag_add("center",1.0,"end")
epsBox.grid(row=1, column=1)

# Transitive Closure Iteration
TCiterLabel = Label(currentSettingFrame, text = "Transitive Closure Iteration(s): ")
TCiterLabel.grid(row=0, column=2, sticky=W, padx=15, pady=10)
TCiterBox = Text(currentSettingFrame, height=2, width=5)
TCiterBox.tag_configure("center",justify='center', font=("Comic Sans MS", 15))
TCiterBox.tag_add("center",1.0,"end")
TCiterBox.grid(row=0, column=3)

# Program Iteration
progIteLabel = Label(currentSettingFrame, text = "Program Iteration(s): ")
progIteLabel.grid(row=1, column=2, sticky=W, padx=15, pady=10)
progIteBox = Text(currentSettingFrame, height=2, width=5)
progIteBox.tag_configure("center",justify='center', font=("Comic Sans MS", 15))
progIteBox.tag_add("center",1.0,"end")
progIteBox.grid(row=1, column=3)

# Current Job Running
jobLabel = Label(currentSettingFrame, text = "Current Job Running")
jobLabel.grid(row=0, column=4, sticky=S, padx=15, pady=10)
jobBox = Text(currentSettingFrame, height=2, width=40)
jobBox.tag_configure("center",justify='center', font=("Comic Sans MS", 15))
jobBox.tag_add("center",1.0,"end")
jobBox.grid(row=1, column=4)

#=============== Other Buttons ===============
# Parent Frame
subexFrame = LabelFrame(mainframe, relief="ridge")
subexFrame.grid(row=4, column=2, padx=5, pady=5, sticky='e') 

# 1. Submit Button to Execute a bash script 
submitJobButton = Button(subexFrame, text="Submit Job", bg="#82CC6C",relief="groove",command=lambda: start_popup1_thread(None)) #, width=10)
submitJobButton.grid(row=0, column=0, sticky='w', padx=10, pady=10)

# 2. Exit Button
exitButton = Button(subexFrame, text="Exit", bg="red", relief="groove",command=popup2) #, width=10)
exitButton.grid(row=0, column=1, sticky='e', padx=5, pady=5)


#=============== Window RESET Button ===============
FRAMES = [
    mainframe,
    appFrame,
    currentSettingFrame,
    psummFrame,
    execFrame
    ]

def resetWin():
    # Ask user to be sure about resetting
    response = messagebox.askokcancel(message="Caution: \nAll running jobs will be terminated. \n\nAre you sure you want to continue? ") 
    if response:
        # Inside Other Buttons Frame
        for items in FRAMES:
            for widget in items.winfo_children():
                if isinstance(widget, Entry): # If this is an Entry widget class
                    widget.delete(0,'end')   # delete all entries 
                if isinstance(widget, ttk.Combobox):
                    widget.delete(0,'end') 
                if isinstance(widget, Text):
                    widget.delete('1.0','end') # Delete from position 0 till end 
                if isinstance(widget, Checkbutton):
                    widget.deselect()
                if isinstance(widget, Radiobutton):
                    appType.set(None)
                if isinstance(widget, OptionMenu):
                    clusVar.set(None)                
                # Terminate Driver File
                #sp.Popen(["bash", "kill -9", "script.sh"]) 

# 3. Reset Window Button
winResetButton = Button(mainframe, text="Reset", relief="groove", command=resetWin)#, width=10)
winResetButton.grid(row=0, column=1, columnspan=2, sticky='e', padx=80, pady=28)

root.mainloop()