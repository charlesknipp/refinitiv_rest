from multiprocessing import Process,Pipe
from time import sleep,time
from sys import stdout
from signal import signal,SIGPIPE,SIG_DFL

## TRACKER BASE CLASS #########################################################

class TrackerBase:
    # this is incredibly cursed, but it works...unlike tqdm
    ansi_fmt = "\x1b[%dB\x1b[2K%s\r\x1b[%dA"
    
    def __init__(self,position,description):
        self.position = position
        self.description = description
        self.timer = time()

    def formatter(self,message,elapsed_time=None):
        if message is None:
            message = self.description
        
        if elapsed_time is not None:
            message = "%20s %7.2f" % (message,elapsed_time)

        return self.ansi_fmt % (self.position,message,self.position)

    def display(self,message=None,show_time=True):
        if show_time:
            elapsed_time = time() - self.timer
            progress = self.formatter(message,elapsed_time)
        else:
            progress = self.formatter(message)
        
        stdout.write(progress)
        stdout.flush()

        # prevents flickering in the console
        sleep(0.01)
    
    def update(self):
        # this is just an alias
        self.display(None,True)
    
    def close(self,message):
        self.display(message)
        sleep(3)

        # clear once done
        self.display("",show_time=False)

    def reset(self,description=None):
        if description is not None:
            self.description = description

        self.timer = time()

## ASYNC TRACKING FUNCTIONS ###################################################

def track_progress(pos,task_desc,pipe):
    # recieve a starting message
    open_msg = "%s %s" % (pipe.recv(),task_desc)

    # define tracker and show elapsed time
    tracker = TrackerBase(pos,open_msg)
    while (not pipe.poll()):
        tracker.update()

    # close it out and clear the terminal
    close_msg = "%s %s" % (pipe.recv(),task_desc)
    tracker.close(close_msg)

def async_tracking(pos,task_desc):
    pipe_in,pipe_out = Pipe()
    tracker = Process(
        target = track_progress,
        args = (pos,task_desc,pipe_out)
    )

    return tracker,pipe_in

class Tracker():
    def __init__(self,position,task_description):
        self.position = position
        self.task_description = task_description

        # to prevent broken pipe errors
        signal(SIGPIPE,SIG_DFL)

    def begin_tracking(self,message=""):
        # store these both under one roof to minimize confusion
        self.tracker,self.pipe = async_tracking(
            self.position,
            self.task_description
        )

        self.pipe.send(message)
        self.tracker.start()

    def end_tracking(self,message):
        try:
            self.pipe.send(message)
            self.tracker.join()
        except:
            raise "Tracking has yet to begin!"

## MAIN #######################################################################

if __name__ == "__main__":
    tracker = Tracker(1,"test process")
    tracker.begin_tracking("Processing")
    sleep(3)
    tracker.end_tracking("Processed")