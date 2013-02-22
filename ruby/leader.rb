require "thread"
require "timeout"
require "message"
require "leader_protocol.rb"
require "instance_record.rb"

module ToyPaxos

#
# These two classes listen for heartbeats from other leaders
# and, if none appear, tell this leader that it should
# be the primary
#
class HeartbeatListener
  def initialize(leader)
    @que = Queue.new
    @abort = false
    @leader = leader
  end

  def add_hb(message)
    @que.push(message)
  end

  def do_abort
    @abort = true
  end

  def start
    while not @abort
      begin
        timeout(2) do
          hb = @que.pop
          @leader.do_hb_action(hb)
        end
      rescue TimeoutError => e
        @leader.do_hb_timeout
      end
    end
  end
end

class HeartbeatSender
  def initialize(leader)
    @leader = leader
    @abort = false
  end

  def do_abort
    @abort = true
  end

  def start
    while not @abort
      sleep(1)
      if @leader.primary
        msg = Message.new(Message::HEARTBEAT)
        @leader.leaders do |leader|
          @leader.send_message(msg, leader)
        end
      end
    end
  end
end

class Leader
  attr_reader :leaders, :acceptors, :primary, :logger

  def initialize(logger, port, leaders, acceptors)
    @logger = logger
    @port = port
    @leaders = leaders
    @acceptors = acceptors
    @primary = false
    @proposal_count = 0
    @msg_pump = MessagePump.new(port)
    @instances = {}
    @hb_listener = HeartbeatListener.new(self)
    @hb_sender = HeartbeatSender.new(self)
    @highest = -1
    # The last time we tried to fix up any gaps
    @lasttime = Time.now
  end

  def do_hb_action(hb)
    # Easy way to settle conflicts
    # if your port number is bigger than mine,
    # you get to be the leader
    if hb.source > @port
      set_primary(false)
    end
  end

  def do_hb_timeout
    set_primary(true)
  end

  def send_message(message, dest)
    message.source = @port
    message.to = dest
    logger.debug "leader(#{@port}) send #{message}"
    @msg_pump.send_message(message)
  end

  def start
    t1 = Thread.new do
      begin
        @hb_sender.start( )
        logger.info "thread(hb_sender) finish"
      rescue => e
        logger.error e
        logger.error e.backtrace
      end
    end
    t2 = Thread.new do
      begin
        @hb_listener.start( )
        logger.info "thread(hb_listener) finish"
      rescue => e
        logger.error e
        logger.error e.backtrace
      end
    end
    t3 = Thread.new do
      begin
        @msg_pump.start() do |message|
          if message
            recv_message(message)
          else
            # Only run every 15s otherwise you run the risk of
            # cutting good protocols off in their prime :(
            if @primary and (Time.now - @lasttime > 15.0)
              find_and_fill_gaps
              collect_garbage
            end
          end
        end
        logger.info "thread(message_pump) finish"
      rescue => e
        logger.error e
        logger.error e.backtrace
      end
    end
  end

  def stop
    @hb_sender.do_abort
    @hb_listener.do_abort
    @msg_pump.do_abort
  end

  def set_primary(primary)
    if @primary != primary
      # Only print if something's changed
      if primary
        logger.info "I (#{@port}) am the leader"
      else
        logger.info "I (#{@port}) am NOT the leader"
      end
    end
    @primary = primary
  end

  def quorum_size
    (@acceptors.size / 2) + 1
  end

  def get_instance_value(instance_id)
    if @instances.has_key? instance_id
      return @instances[instance_id].value
    else
      return nil
    end
  end

  def get_history
    histories = []
    (1..(@highest+1)).each do |n|
      v = get_instance_value(n)
      histories.push(v)
    end
    return histories
  end

  def get_num_accepted
    histories = get_history
    a = []
    histories.each do |h|
      a.push(h) if h
    end
    a.size
  end

  def notify_leader(protocol, message)
    # Protocols call this when they're done
    if protocol.state == LeaderProtocol::STATE_ACCEPTED
      logger.info "Protocol instance #{message.instance_id} accepted with value #{message.value}"
      @instances[message.instance_id].accepted = true
      @instances[message.instance_id].value = message.value
      @highest = message.instance_id > @highest ?
                 message.instance_id : @highest
    elsif protocol.state == LeaderProtocol::STATE_REJECTED
      # Look at the message to find the value, and then retry
      # Eventually, assuming that the acceptors will accept some value for
      # this instance, the protocol will complete.
      # TODO message.highestPID[1] ?
      @proposal_count = @proposal_count > message.proposal_id[1] ?
                        @proposal_count : message.proposal_id[1]
      new_proposal(message.value)
    elsif protocol.state == LeaderProtocol::STATE_UNACCEPTED
      # do nothing
    else
      logger.warn "protocol.state #{protocol.state} is unexpected."
      #raise "protocol.state #{protocol.state} is unexpected."
    end
  end

private
  def find_and_fill_gaps
    # if no message is received, we take the chance to do a little cleanup
    (1..(@highest)).each do |n|
      if get_instance_value(n) == nil
        logger.debug "Filling in gap #{n}"
        new_proposal(0, n)
        # This will either eventually commit an already accepted value,
        # or fill in the gap with 0 or no-op
      end
    end
    @lasttime = Time.now
  end

  def collect_garbage
    @instances.each do |k, v|
      v.clean_protocols
    end
  end

  def recv_message(message)
    raise "message is not nil" unless message

    if message.command == Message::HEARTBEAT
      logger.debug "HeartBeat received at #{@port} #{@highest}"
      @hbListener.add_hb(message)

    elsif message.command == Message::EXT_PROPOSE
      logger.debug "External proposal received at #{@port} #{@highest}"
      if @primary
        new_proposal(message.value)
      end
      # else ignore - we're getting  proposals when we're not the primary
      # what we should do, if we were being kind,
      # is reply with a message saying 'leader has changed'
      # and giving the address of the new one.
      # However, we might just as well have failed.

    elsif @primary and message.command != Message::ACCEPTOR_ACCEPT
      logger.debug "Command #{message.command} received at #{@port} #{@highest}"
      @instances[message.instance_id].get_protocol(message.proposal_id).
                                      do_transition(message)
      # It's possible that, while we still think we're the primary,
      # we'll get a accept message that we're only listening in on.
      # We are interested in hearing all accepts,
      # so we play along by pretending we've got the protocol
      # that's getting accepted and listening for a quorum as per usual

    elsif message.command == Message::ACCEPTOR_ACCEPT
      logger.debug "Accept received at #{@port} #{@highest}"
      if !@instances.has_key? message.instance_id
        @instances[message.instance_id] = InstanceRecord.new(logger)
      end
      record = @instances[message.instance_id]
      protocol = nil
      if !record.protocols.has_key? message.proposal_id
        protocol = LeaderProtocol.new logger, self
        # We just massage this protocol to be in the waiting-for-accept state
        protocol.state = LeaderProtocol::STATE_AGREED
        protocol.proposal_id = message.proposal_id
        protocol.instance_id = message.instance_id
        protocol.value = message.value
        record.add_protocol(protocol)
      else
        protocol = record.get_protocol(message.proposal_id)
      end
      # Should just fall through to here if we initiated this protocol instance
      protocol.do_transition(message)
    else
      raise "message.command #{message.command} is unexpected."
    end
  end

  def new_proposal(value, instance = nil)
    if instance == nil
      @highest += 1
      instance_id = @highest
    else
      instance_id = instance
    end
    @proposal_count += 1
    id = [@port, @proposal_count]
    if @instances.has_key? instance_id
      record = @instances[instance_id]
    else
      record = InstanceRecord.new(logger)
      @instances[instance_id] = record
    end

    protocol = LeaderProtocol.new logger, self
    protocol.propose(value, id, instance_id)
    record.add_protocol(protocol)
  end

end
end
