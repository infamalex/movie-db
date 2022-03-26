import os
import curses

TOP_PADDING=5
help_text = [
    "Commands:",
    "watched [userid] ",
    "command 2:",
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

def draw_table(screen, data, start):
    height, width = screen.getmaxyx()
    
    if len(data) < 0:
        return
    table = data[start:start+height-3]
    titles = [d for d in table[0]]

    
    ml = [max(len(l),max(map(lambda x : len(x[l]),table))) for l in titles] #get column widths
    index_width = max(len(str(start+height)),3)#width of the row number column

    
    screen.addstr(1,0,"{:>{}}|".format("row",index_width),curses.A_BOLD)
    x = index_width + 1
    #draw title
    for col in range(len(titles)):
        screen.addstr(1,x,"{:>{}}|".format(titles[col],ml[col]),curses.A_BOLD)
        x+=ml[col]+1
    screen.addstr(0, 0, '-'*width)
    screen.addstr(2, 0, '-'*width)

    #draw table
    for row in range(len(table)):
        screen.addstr(row+3,0,"{:>{}}|".format(str(start+row),index_width),curses.A_UNDERLINE)
        x = index_width + 1
        line = table[row]
        for col in range(len(titles)):
            screen.addstr(row+3,x,"{:>{}}|".format(line[titles[col]],ml[col]),curses.A_UNDERLINE)
            x+=ml[col]+1

    return sum(ml) + len(titles) + index_width + 2 #return total width
            



def main(screen):
    k = 0
    cursor_x = 0
    table_y = 0
    table_x = 0 
    table_width=256

    command = ""
    # Clear and refresh the screen for a blank canvas
    screen.clear()
    screen.refresh()

    info = curses.newwin(0,0,0,0)
    display = curses.newpad(100, 100)

    # Start colors in curses
    curses.start_color()
    curses.init_pair(1, curses.COLOR_CYAN, curses.COLOR_BLACK)
    curses.init_pair(2, curses.COLOR_RED, curses.COLOR_BLACK)
    curses.init_pair(3, curses.COLOR_BLACK, curses.COLOR_WHITE)

    # Loop where k is the last character pressed
    while (True):
        # Initialization
        screen.clear()
        height, width = screen.getmaxyx()
        

        #draw helptext
        info.clear()
        c_width = min(help_width+2,width)
        info.resize(height-1, c_width)
        info.mvwin(0, width-c_width)
        
        for i in range(min(len(help_text),height)):
            info.addstr(4+i, 2, help_text[i][:c_width-1])
        for i in range(height-1):
            info.addch(i,0,'|')

        data =[]
        for i in range(100):
            data.append(dict())
            for j in range(35):
                data[-1][str(j)]=str(i*j)

        

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
            table_y += display.getyx()[0]
        elif k == curses.KEY_PPAGE:
            table_y -= display.getyx()[0]
        elif k == curses.KEY_BACKSPACE:
            command = command[:-1]
        elif k == curses.KEY_END:
            table_y = len(data)-1
        elif k == curses.KEY_END:
            table_y = 0
        elif k != 0:
            command+=chr(k)

        table_x = min(table_width-width+c_width, table_x)
        table_x = max(0, table_x)

        table_y = max(0, table_y)
        table_y = min(len(data)-1, table_y)

        display.clear()
        display.resize(height-TOP_PADDING,256)
        table_width = draw_table(display,data,table_y)

        # display current command
        screen.addstr(2,4,command[:width - c_width - 1])

        # Refresh the screen
        screen.refresh()
        info.refresh()
        display.refresh(0,table_x,TOP_PADDING,0,height-2,width-c_width-1)

        # Wait for next input
        k = screen.getch()

        


curses.wrapper(main)