from segment import Segment


# #################################################################################################################### #
# RDTLayer                                                                                                             #
#                                                                                                                      #
# Description:                                                                                                         #
# The reliable data transfer (RDT) layer is used as a communication layer to resolve issues over an unreliable         #
# channel.                                                                                                             #
#                                                                                                                      #
#                                                                                                                      #
# Notes:                                                                                                               #
# This file is meant to be changed.                                                                                    #
#                                                                                                                      #
#                                                                                                                      #
# #################################################################################################################### #


class RDTLayer(object):
    # ################################################################################################################ #
    # Class Scope Variables                                                                                            #
    #                                                                                                                  #
    #                                                                                                                  #
    #                                                                                                                  #
    #                                                                                                                  #
    #                                                                                                                  #
    # ################################################################################################################ #
    DATA_LENGTH = 4 # in characters                     # The length of the string data that will be sent per packet...
    FLOW_CONTROL_WIN_SIZE = 15 # in characters          # Receive window size for flow-control
    sendChannel = None
    receiveChannel = None
    dataToSend = ''
    currentIteration = 0                                # Use this for segment 'timeouts'
    # Add items as needed

    # We need to differentiate the server and client
    thisIsServer = None
    thisIsClient = None

    # Server saves the final data in this container
    dataReceived = ""

    # Segment container, will be created by server
    listSegments = None

    # Flow control variables
    flowControlStart = 0
    flowControlEnd = 4

    # ################################################################################################################ #
    # __init__()                                                                                                       #
    #                                                                                                                  #
    #                                                                                                                  #
    #                                                                                                                  #
    #                                                                                                                  #
    #                                                                                                                  #
    # ################################################################################################################ #
    def __init__(self):
        self.sendChannel = None
        self.receiveChannel = None
        self.dataToSend = ''
        self.currentIteration = 0
        # Add items as needed

        # In rdt_main, the side that receives dataToSend is client. So, every object will be initialized as server, and
        # the object that receives dataToSend will be set as "client".
        self.thisIsServer = True
        self.thisIsClient = False

    # ################################################################################################################ #
    # setSendChannel()                                                                                                 #
    #                                                                                                                  #
    # Description:                                                                                                     #
    # Called by main to set the unreliable sending lower-layer channel                                                 #
    #                                                                                                                  #
    #                                                                                                                  #
    # ################################################################################################################ #
    def setSendChannel(self, channel):
        self.sendChannel = channel

    # ################################################################################################################ #
    # setReceiveChannel()                                                                                              #
    #                                                                                                                  #
    # Description:                                                                                                     #
    # Called by main to set the unreliable receiving lower-layer channel                                               #
    #                                                                                                                  #
    #                                                                                                                  #
    # ################################################################################################################ #
    def setReceiveChannel(self, channel):
        self.receiveChannel = channel

    # ################################################################################################################ #
    # setDataToSend()                                                                                                  #
    #                                                                                                                  #
    # Description:                                                                                                     #
    # Called by main to set the string data to send                                                                    #
    #                                                                                                                  #
    #                                                                                                                  #
    # ################################################################################################################ #
    def setDataToSend(self,data):
        self.dataToSend = data

        # In rdt_main, the side that receives dataToSend is client. So, the object that receives dataToSend will be set
        # as "client".
        self.thisIsServer = False
        self.thisIsClient = True

    # ################################################################################################################ #
    # getDataReceived()                                                                                                #
    #                                                                                                                  #
    # Description:                                                                                                     #
    # Called by main to get the currently received and buffered string data, in order                                  #
    #                                                                                                                  #
    #                                                                                                                  #
    # ################################################################################################################ #
    def getDataReceived(self):
        # ############################################################################################################ #
        # Identify the data that has been received...

        # print('getDataReceived(): Complete this...')

        # ############################################################################################################ #
        if self.thisIsClient is False and self.thisIsServer is True:
            return self.dataReceived

    # ################################################################################################################ #
    # processData()                                                                                                    #
    #                                                                                                                  #
    # Description:                                                                                                     #
    # "timeslice". Called by main once per iteration                                                                   #
    #                                                                                                                  #
    #                                                                                                                  #
    # ################################################################################################################ #
    def processData(self):
        self.currentIteration += 1
        self.processSend()
        self.processReceiveAndSendRespond()

    # ################################################################################################################ #
    # processSend()                                                                                                    #
    #                                                                                                                  #
    # Description:                                                                                                     #
    # Manages Segment sending tasks                                                                                    #
    #                                                                                                                  #
    #                                                                                                                  #
    # ################################################################################################################ #
    def processSend(self):

        # You should pipeline segments to fit the flow-control window
        # The flow-control window is the constant RDTLayer.FLOW_CONTROL_WIN_SIZE
        # The maximum data that you can send in a segment is RDTLayer.DATA_LENGTH
        # These constants are given in # characters

        # Somewhere in here you will be creating data segments to send.
        # The data is just part of the entire string that you are trying to send.
        # The seqnum is the sequence number for the segment (in character number, not bytes)

        # If the segment list is not created and this is client sending the data, prepare the segment list
        if (self.listSegments is None) and (self.thisIsClient is True and self.thisIsServer is False):
            listPacketSizes = self.calculatePacketSizes()
            listDividedData = self.divideDataToSend(listPacketSizes)
            listSequenceNumbers = self.calculatePacketSequenceNumbers(listDividedData)
            listSegments = self.createSegments(listDividedData, listSequenceNumbers)
            self.listSegments = listSegments

        # Client sends data
        if self.thisIsClient is True and self.thisIsServer is False:
            # call flow control window checker
            for segment in self.listSegments[self.flowControlStart: self.flowControlEnd]:
                print("Sending Segment:", segment.to_string())
                self.sendChannel.send(segment)

            self.flowControlStart += 4
            self.flowControlEnd += 4

        # seqnum = "0"
        # data = "x"
        # segmentSend = Segment()
        #
        # # ############################################################################################################ #
        # # Display sending segment
        # segmentSend.setData(seqnum,data)
        # print("Sending segment: ", segmentSend.to_string())
        #
        # # Use the unreliable sendChannel to send the segment
        # self.sendChannel.send(segmentSend)

    def flowControlWindowChecker(self):
        """Update the start and end points for flow control window"""
        for segment in self.listSegments:
            pass

    def calculatePacketSizes(self):
        """Return a list of sizes that contains max number of characters per packet in flow control window size"""

        # In case of DATA_LENGTH = 4 and FLOW_CONTROL_WIN_SIZE = 15, listPacketCharSize = [4, 4, 4, 3]. This will be
        # later used for generating packets.
        listPacketCharSizes = []

        numberOfPackets = self.FLOW_CONTROL_WIN_SIZE // self.DATA_LENGTH

        for _ in range(numberOfPackets):
            listPacketCharSizes.append(self.DATA_LENGTH)

        if self.FLOW_CONTROL_WIN_SIZE % self.DATA_LENGTH != 0:
            listPacketCharSizes.append(self.FLOW_CONTROL_WIN_SIZE - numberOfPackets * self.DATA_LENGTH)

        # print(listPacketCharSizes)
        return listPacketCharSizes

    def divideDataToSend(self, packetSizes):
        """
        Partition dataToSend to a list
        :param packetSizes: list of packet sizes calculated according to the flow control window size
        :return: dataToSend partitioned as a list of characters according to the given sizes
        """
        dividedData = []
        index = 0

        while index < len(self.dataToSend):
            for size in packetSizes:
                if index + size > len(self.dataToSend):
                    chunk = self.dataToSend[index:]
                    dividedData.append(chunk)
                    index += len(chunk)
                    break
                else:
                    # If enough characters are left, proceed as usual
                    chunk = self.dataToSend[index:index + size]
                    dividedData.append(chunk)
                    index += size

        # print(dividedData)
        return dividedData

    def calculatePacketSequenceNumbers(self, partitionedData):
        """
        Return a list of sequence numbers for packet generation
        :param partitionedData: Sequence numbers will be generated for this list
        :return: A list of sequence numbers
        """
        index = 0
        seqNum = 0
        listSeqNumbers = [index]

        for element in partitionedData:
            seqNum += len(element)
            listSeqNumbers.append(seqNum)

        # We do not need to calculate the sequence number after the last packet
        listSeqNumbers.pop()

        # print(listSeqNumbers)
        return(listSeqNumbers)

    def createSegments(self, listData, listSeqNum):
        """
        Creates a list of segment of objects containing information from the following lists
        :param listData: a list of partitioned data according to the specifications
        :param listSeqNum: a list of sequence numbers calculated using listData
        :return: a list of segment objects to be sent
        """
        listSegments = []

        for seqNum, data in zip(listSeqNum, listData):
            segment = Segment()
            segment.setData(seqNum, data)
            listSegments.append(segment)

        # for segment in listSegments:
        #     segment.printToConsole()

        return listSegments

    # ################################################################################################################ #
    # processReceive()                                                                                                 #
    #                                                                                                                  #
    # Description:                                                                                                     #
    # Manages Segment receive tasks                                                                                    #
    #                                                                                                                  #
    #                                                                                                                  #
    # ################################################################################################################ #
    def processReceiveAndSendRespond(self):

        # This call returns a list of incoming segments (see Segment class)...
        listIncomingSegments = self.receiveChannel.receive()

        # If this is server (the side that receives only data)
        if self.thisIsClient is False and self.thisIsServer is True:

            if listIncomingSegments:
                for segment in listIncomingSegments:
                    # extract payload from each segment
                    segmentPayload = segment.payload
                    # add this data to server data container
                    self.dataReceived += segmentPayload


        # ############################################################################################################ #
        # What segments have been received?
        # How will you get them back in order?
        # This is where a majority of your logic will be implemented
        # print('processReceive(): Complete this...')

        # ############################################################################################################ #
        # How do you respond to what you have received?
        # How can you tell data segments apart from ack segemnts?
        # print('processReceive(): Complete this...')

        # Somewhere in here you will be setting the contents of the ack segments to send.
        # The goal is to employ cumulative ack, just like TCP does...
        acknum = "0"


        # ############################################################################################################ #
        # Display response segment
        # segmentAck.setAck(acknum)
        # print("Sending ack: ", segmentAck.to_string())
        #
        # # Use the unreliable sendChannel to send the ack packet
        # self.sendChannel.send(segmentAck)
