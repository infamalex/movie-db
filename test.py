import os
import curses

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
def main(screen):
    k = 0
    cursor_x = 0
    cursor_y = 0
    # Clear and refresh the screen for a blank canvas
    screen.clear()
    screen.refresh()

    info = curses.newwin(0,0,0,0)

    # Start colors in curses
    curses.start_color()
    curses.init_pair(1, curses.COLOR_CYAN, curses.COLOR_BLACK)
    curses.init_pair(2, curses.COLOR_RED, curses.COLOR_BLACK)
    curses.init_pair(3, curses.COLOR_BLACK, curses.COLOR_WHITE)

    # Loop where k is the last character pressed
    while (k != ord('q')):
        # Initialization
        screen.clear()
        height, width = screen.getmaxyx()
        

        info.clear()
        c_width = help_width+2
        info.resize(height-1, c_width)
        info.mvwin(0, width-c_width)
        
        for i in range(len(help_text)):
            info.addstr(4+i, 2, help_text[i][:c_width-1])
        for i in range(height-1):
            info.addch(i,0,'|')


        if k == curses.KEY_DOWN:
            cursor_y = cursor_y + 1
        elif k == curses.KEY_UP:
            cursor_y = cursor_y - 1
        elif k == curses.KEY_RIGHT:
            cursor_x = cursor_x + 1
        elif k == curses.KEY_LEFT:
            cursor_x = cursor_x - 1

        cursor_x = max(0, cursor_x)
        cursor_x = min(width-1, cursor_x)

        cursor_y = max(0, cursor_y)
        cursor_y = min(height-1, cursor_y)

        # Declaration of strings
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
        screen.move(cursor_y, cursor_x)

        # Refresh the screen
        info.refresh()
        screen.refresh()
        

        # Wait for next input
        k = screen.getch()

        


curses.wrapper(main)