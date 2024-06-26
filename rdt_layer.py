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
    DATA_LENGTH = 4  # in characters                     # The length of the string data that will be sent per packet...
    FLOW_CONTROL_WIN_SIZE = 15  # in characters          # Receive window size for flow-control
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

    # Flow control variables, at the start of data transfer
    flowControlStart = 0
    flowControlEnd = 4

    # Variable to store ack numbers received, for client
    ackNumberContainer = []

    # Sent ack number container, for server
    sentAckNumberContainer = []

    # Variable to store the sequence number of the last data segment received, for server
    serverLastSeqNum = 0

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
        self.countSegmentTimeouts = 0

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
    def setDataToSend(self, data):
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

        # If the segment list is not created and this is client sending the data, prepare the segment list
        # I know this is inefficient but at the beginning I had not noticed that the channel was able to corrupt
        # previously saved packets, therefore this list of segment object is created again every time
        if self.thisIsClient is True and self.thisIsServer is False:
            listPacketSizes = self.calculatePacketSizes()
            listDividedData = self.divideDataToSend(listPacketSizes)
            listSequenceNumbers = self.calculatePacketSequenceNumbers(listDividedData)
            listSegments = self.createSegments(listDividedData, listSequenceNumbers)
            self.listSegments = listSegments

        # Client
        if self.thisIsClient is True and self.thisIsServer is False:

            # Client receives acks from server
            if len(self.receiveChannel.receiveQueue) != 0:
                listOfAckSegments = self.receiveChannel.receive()

                # Check ack segments
                for incomingSegment in listOfAckSegments:

                    # extract ack number from each segment
                    incomingSegmentAckNumber = incomingSegment.acknum
                    # add this data to server ack number container
                    self.ackNumberContainer.append(incomingSegmentAckNumber)

            # After checking acks, send messages

            # This should handle the cumulative ack by finding the highest ack number then shifting the control window
            if self.ackNumberContainer:
                ackToFind = max(self.ackNumberContainer)  # Highest char number ack'ed
                for segment in self.listSegments:
                    if ackToFind == segment.seqnum:
                        self.flowControlStart = self.listSegments.index(segment) + 1
                        self.flowControlEnd = self.flowControlStart + 4
                        # print("Flow control window:")
                        # print(self.listSegments[self.flowControlStart].seqnum,
                        #       self.listSegments[self.flowControlEnd].seqnum)
            else:
                self.flowControlStart = 0
                self.flowControlEnd = self.flowControlStart + 4

            for segment in self.listSegments[self.flowControlStart: self.flowControlEnd]:
                print("Sending Segment:", segment.to_string())
                self.sendChannel.send(segment)

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

        return listSeqNumbers

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

        # If this is server (the side that receives only data segments), never receives ack segments
        if self.thisIsClient is False and self.thisIsServer is True:

            if listIncomingSegments:
                for incomingSegment in listIncomingSegments:

                    # If no ack segments received, then first segment received should have seqnum zero
                    if len(self.sentAckNumberContainer) == 0 and incomingSegment.seqnum != 0:
                        break

                    # Check checksum and seqnum for this segment
                    if (incomingSegment.checkChecksum() is True and
                            # segment is in order
                            incomingSegment.seqnum <= self.serverLastSeqNum + 4 and
                            # segment is not duplicate
                            incomingSegment.seqnum not in self.sentAckNumberContainer):

                        # extract payload from each segment
                        incomingSegmentPayload = incomingSegment.payload
                        # add this data to server data container
                        self.dataReceived += incomingSegmentPayload
                        # extract the seqnum from the segment
                        incomingSegmentSeqNum = incomingSegment.seqnum
                        # assign this data to server data container
                        self.serverLastSeqNum = incomingSegmentSeqNum

                        # Now send the ack segments for correctly received data segment
                        segmentAck = Segment()
                        segmentAck.setAck(self.serverLastSeqNum)
                        print("Sending ack: ", segmentAck.to_string())
                        # The following ack numbers are for segments with payload already received,
                        # will use this to detect duplicate packets
                        self.sentAckNumberContainer.append(self.serverLastSeqNum)
                        self.sendChannel.send(segmentAck)

                    else:  # discard the segment by not using its payload

                        # If the server is expecting the first packet and its corrupt
                        if self.serverLastSeqNum == 0:
                            print("First packet is corrupt")
                            # Do not send any ack
                            break

                        # We should send the sequence number of last data packet received correctly
                        segmentAck = Segment()
                        segmentAck.setAck(self.serverLastSeqNum)
                        print("Data Error - Sending previous ack: ", segmentAck.to_string())
                        self.sendChannel.send(segmentAck)
