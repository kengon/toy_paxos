module ToyPaxos

class LeaderProtocol
  STATE_UNDEFINED = -1
  STATE_PROPOSED = 0
  STATE_AGREED = 1
  STATE_REJECTED = 2
  STATE_ACCEPTED = 3
  STATE_UNACCEPTED = 4

  attr_accessor :state, :proposal_id, :instance_id, :value
  attr_reader :logger

  def initialize(logger, leader)
    @logger = logger
    @leader = leader
    @state = STATE_UNDEFINED
    @proposal_id = [-1,-1]
    @agreecount, @acceptcount = [0,0]
    @rejectcount, @unacceptcount = [0,0]
    @instance_id = -1
    @highestseen = [0,0]
    @value = nil
  end

  def propose(value, pid, instance_id)
    @proposal_id = pid
    @instance_id = instance_id
    @value = value

    message = Message.new(Message::PROPOSE)
    message.proposal_id = pid
    message.instance_id = instance_id
    message.value = value
    @leader.acceptors.each do |acceptor|
      @leader.send_message(message, acceptor)
    end
    @state = STATE_PROPOSED
  end

  def do_transition(message)
    # We run the protocol like a simple state machine.
    # It's not always okay to error on unexpected inputs, however,
    # due to message delays, so we silently
    # ignore inputs that we're not expecting.
    if @state == STATE_PROPOSED
      do_transition_as_proposed(message)
    elsif @state == STATE_AGREED
      do_transition_as_agreed(message)
    else
      logger.warn "@state #{@state} is unexpected."
      #raise "@state #{@state} is unexpected."
    end
  end

private
  def do_transition_as_proposed(message)
    if message.command == Message::ACCEPTOR_AGREE
      @agreecount += 1
      if @agreecount >= @leader.quorum_size()
        # puts "Achieved agreement quorum, last value replied was: #{message.value}"

        # If it's none, can do what we like.
        # Otherwise we have to take the highest seen proposal
        if message.value != nil
          if message.sequence[0] > @highestseen[0] or
             (message.sequence[0] == @highestseen[0] and
              message.sequence[1] > @highestseen[1])
              @value = message.value
              @highestseen = message.sequence
          end
        end
        @state = STATE_AGREED

        # Send 'accept' message to group
        msg = Message.new(Message::ACCEPT)
        msg.copy_as_reply(message)
        msg.value = @value
        # msg.leaderID = msg.to #TODO leaderID?
        @leader.acceptors.each do |acceptor|
          @leader.send_message(msg, acceptor)
        end
        @leader.notify_leader(self, message)
      end
    elsif message.command == Message::ACCEPTOR_REJECT:
      @rejectcount += 1
      if @rejectcount >= @leader.quorum_size()
        @state = STATE_REJECTED
        @leader.notify_leader(self, message)
      end
    else
      assert false, "message.command #{message.command} is unexpected."
    end
  end

  def do_transition_as_agreed(message)
    if message.command == Message::ACCEPTOR_ACCEPT
      @acceptcount += 1
      if @acceptcount >= @leader.quorum_size()
        @state = STATE_ACCEPTED
        @leader.notify_leader(self, message)
      end
    elsif message.command == Message::ACCEPTOR_UNACCEPT
      @unacceptcount += 1
      if @unacceptcount >= @leader.quorum_size()
        @state = STATE_UNACCEPTED
        @leader.notify_leader(self, message)
      end
    else
      logger.warn "message.command #{message.command} is unexpected."
      #raise "message.command #{message.command} is unexpected."
    end
  end
end
end
