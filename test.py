import os
import curses
import movies
from re import match
from pyspark.sql.functions import col

TOP_PADDING=3
help_text = [
    "Commands:",
    "watch [userid] ",
    "sort col_name",
    "sortd col_name",
    "max len",
    "all",
    'movie [id|"title"]',
    "comp id1 id2", 
    "quit"
]
help_width = max(map(len,help_text))

def draw_info(screen):
    height, s_width = screen.getmaxyx()
    c_width = help_width+3
    info = curses.newwin(height-1, c_width, 0, s_width-c_width)
    for i in range(len(help_text)):
        info.addstr(4+i, 2, help_text[i][:c_width-1])

def draw_centre(screen, text):
    title = "Curses example"[:width-1]
    subtitle = "Written by Clay McLeod"[:width-1]
    keystr = "Last key pressed: {}".format(k)[:width-1]
    statusbarstr = "Press 'q' to exit | STATUS BAR | Pos: {}, {}".format(cursor_x, cursor_y)
    if k == 0:
        keystr = "No key press detected..."[:width-1]

    # Centering calculations
    start_x_title = int((width // 2) - (len(title) // 2) - len(title) % 2)
    start_x_subtitle = int((width // 2) - (len(subtitle) // 2) - len(subtitle) % 2)
    start_x_keystr = int((width // 2) - (len(keystr) // 2) - len(keystr) % 2)
    start_y = int((height // 2) - 2)

    # Rendering some text
    whstr = "Width: {}, Height: {}".format(width, height)
    screen.addstr(0, 0, whstr, curses.color_pair(1))

    # Render status bar
    screen.attron(curses.color_pair(3))
    screen.addstr(height-1, 0, statusbarstr)
    screen.addstr(height-1, len(statusbarstr), " " * (width - len(statusbarstr) - 1))
    screen.attroff(curses.color_pair(3))

    # Turning on attributes for title
    screen.attron(curses.color_pair(2))
    screen.attron(curses.A_BOLD)

    # Rendering title
    screen.addstr(start_y, start_x_title, title)

    # Turning off attributes for title
    screen.attroff(curses.color_pair(2))
    screen.attroff(curses.A_BOLD)

    # Print rest of text
    screen.addstr(start_y + 1, start_x_subtitle, subtitle)
    screen.addstr(start_y + 3, (width // 2) - 2, '-' * 4)
    screen.addstr(start_y + 5, start_x_keystr, keystr)

class Table:
    def __init__(self, screen, data) -> None:
        self.screen = screen
        self.data = data
        if len(data) == 0: return
        self.titles = list(data[0].asDict().keys())

        self.ml = [max(len(l),max(map(lambda x : len(str(x[l])),data))) for l in self.titles] #get column widths
        self.index_width = max(len(str(len(data))),3)#width of the row number column

    def draw_table(self,start,max_length):
        
        if len(self.data) == 0 or self.screen == None:
            return 0
        height, width = self.screen.getmaxyx()

        table = self.data[start:start+height-3]

        
        self.screen.attron(curses.A_BOLD)
        self.screen.attron(curses.color_pair(3))
        self.screen.addstr(1,0,"{:>{}}|".format("row",self.index_width))
        x = self.index_width + 1
        #draw title
        for col in range(len(self.titles)):
            length = min(self.ml[col],max_length)
            self.screen.addstr(1,x,"{:>{}}|".format(self.titles[col],length))
            x+=length+1
        self.screen.addstr(0, 0, '-'*width)
        self.screen.addstr(2, 0, '-'*width)
        
        self.screen.attroff(curses.A_BOLD)
        self.screen.attroff(curses.color_pair(3))
        
        
        self.screen.attron(curses.A_UNDERLINE)
        #draw table
        for row in range(len(table)):
            self.screen.addstr(row+3,0,"{:>{}}|".format(str(start+row),self.index_width))
            x = self.index_width + 1
            line = table[row]
            for col in range(len(self.titles)):
                length = min(self.ml[col],max_length)
                self.screen.addstr(row+3,x,
                    "{:>{}}|".format(str(line[self.titles[col]])[:length],length),
                    curses.A_UNDERLINE)
                x+=length+1
        self.screen.attroff(curses.A_UNDERLINE)

        return sum(self.ml) + len(self.titles) + self.index_width + 2 #return total width
            



def main(screen):
    result = None
    table = Table(None,[])
    typed_flag = False
    k = 0
    max_length = 255
    table_y = 0
    table_x = 0 
    table_width=256

    command = ""
    # erase and refresh the screen for a blank canvas
    screen.erase()
    screen.refresh()

    info = curses.newwin(0,0,0,0)
    display = curses.newpad(100, 100)

    # Start colors in curses
    curses.start_color()
    curses.init_pair(1, curses.COLOR_CYAN, curses.COLOR_BLACK)
    curses.init_pair(2, curses.COLOR_RED, curses.COLOR_BLACK)
    curses.init_pair(3, curses.COLOR_BLACK, curses.COLOR_WHITE)

    info.attron(curses.color_pair(2))

    status = "M.D.S.T. | INFO | Message: {}"
    statusmsg = ""

    # Loop where k is the last character pressed
    while (True):
        # Initialization
        curses.curs_set(0)
        screen.erase()
        height, width = screen.getmaxyx()
        c_width = min(help_width+2,width)
        t_heigh = max(0,height - TOP_PADDING)

        #draw helptext
        info.erase()
        info.resize(height-1, c_width)
        info.mvwin(0, width-c_width)
        
        
        info.attron(curses.A_BOLD)
        for i in range(min(len(help_text),height-2)):
            info.addstr(2+i, 2, help_text[i][:c_width-1])
            info.attroff(curses.A_BOLD)

        for i in range(height-1):
            info.addch(i,0,'|')

        #data =[]
        #for i in range(100):
        #    data.append(dict())
        #    for j in range(35):
        #        data[-1][str(j)]=str(i*j)

        

        #enable table scrolling
        if k == curses.KEY_DOWN:
            table_y = table_y + 1
        elif k == curses.KEY_UP:
            table_y = table_y - 1
        elif k == curses.KEY_RIGHT:
            table_x+=1
        elif k == curses.KEY_LEFT:
            table_x -= 1
        elif k == curses.KEY_NPAGE:
            table_y += t_heigh - 4
        elif k == curses.KEY_PPAGE:
            table_y -= t_heigh - 4
        elif k == curses.KEY_BACKSPACE:
            command = command[:-1]
        elif k == curses.KEY_END:
            table_y = len(table.data)-1
        elif k == curses.KEY_HOME:
            table_y = 0
        elif k == curses.KEY_ENTER or k == 10 or k == 13:
            if match("q(uit)? *",command):
                return
            elif result != None and match("^sortd? +\w+ *$",command): #sort table
                col_name = command.split()[1]
                data, msg = result
                if col_name in data.columns: #check if valid column name
                    table = Table(display,
                        data.sort(col(col_name),ascending=command[4]==" "
                    ).collect())
                else:
                    statusmsg = "column name not found"
            elif match("g(oto)? +\d+ *",command):
                table_y = int(command.split()[1])
            elif match("m(ax)? +\d+ *",command):
                max_length = max(1,int(command.split()[1]))
            else:        
                result = movies.handle_command(command)
                if result == None:
                    statusmsg = "ERROR"
                else:
                    data, msg = result
                    table_x = 0
                    table_y = 0
                    table = Table(display,data.collect())
                    statusmsg = msg #table and status
            command = ""
            
        elif 32 <= k <= 127: #all printable ascii
            command+=chr(k)
            typed_flag = True

        #thresholds for tha table position
        table_x = min(table_width-width+c_width, table_x)
        table_x = max(0, table_x)

        table_y = max(0, table_y)
        table_y = min(len(table.data)-1, table_y)

        if not typed_flag: #don't refresh table if last action was to type
            display.erase()
            display.resize(height-TOP_PADDING,511)
            table_width = table.draw_table(table_y,max_length)
        typed_flag = False

        # display current command prompt
        screen.attron(curses.color_pair(1))
        screen.addstr(1,4,"command:> {}".format(command)[:width - c_width - 1])
        screen.attroff(curses.color_pair(1))

        #status bar
        screen.attron(curses.color_pair(3))
        msg = status.format(statusmsg)
        screen.addstr(height - 1, 0, msg)
        screen.addstr(height - 1, len(msg), " " * (width - len(msg) - 1))
        screen.attroff(curses.color_pair(3))

        # Refresh the screen
        screen.refresh()
        info.refresh()
        display.refresh(0,table_x,TOP_PADDING,0,height-2,width-c_width-1)

        # Wait for next input
        k = screen.getch()

        


curses.wrapper(main)