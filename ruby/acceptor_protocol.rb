module ToyPaxos

class AcceptorProtocol
  # State variables
  STATE_UNDEFINED = -1
  STATE_PROPOSAL_RECEIVED = 0
  STATE_PROPOSAL_REJECTED = 1
  STATE_PROPOSAL_AGREED = 2
  STATE_PROPOSAL_ACCEPTED = 3
  STATE_PROPOSAL_UNACCEPTED = 4

  attr_reader :proposal_id, :instance_id, :state

  def initialize(client)
    @client = client
    @state = STATE_UNDEFINED
    @proposal_id = nil
    @instance_id = nil
  end

  def recv_proposal(message)
    if message.command == Message::PROPOSE
      @proposal_id = message.proposal_id
      @instance_id = message.instance_id

      # What's the highest already agreed proposal for this instance?
      port, count = @client.get_highest_agreed_proposal(message.instance_id)
      # Check if this proposal is numbered higher
      if count < @proposal_id[0] or
         (count == @proposal_id[0] and
          port < @proposal_id[1])
        # Send agreed message back, with highest accepted value (if it exists)
        @state = STATE_PROPOSAL_AGREED
        # puts "Agreeing to proposal: #{message.instance_id}, #{message.value}"
        value = @client.get_instance_value(message.instance_id)
        msg = Message.new(Message::ACCEPTOR_AGREE)
        msg.copy_as_reply(message)
        msg.value = value
        msg.sequence = [port, count]
        @client.send_message(msg)
      else
        # Too late, we already told someone else we'd do it
        # Send reject message, along with highest proposal id and its value
        @state = STATE_PROPOSAL_REJECTED
      end
    else
      # error, trying to receive a non-proposal?
    end
  end

  def do_transition(message)
    if @state == STATE_PROPOSAL_AGREED and
       message.command == Message::ACCEPT
      @state = STATE_PROPOSAL_ACCEPTED

      # Could check on the value here,
      # if we don't trust leaders to honour what we tell them
      # send reply to leader acknowledging
      msg = Message.new(Message::ACCEPTOR_ACCEPT)
      msg.copy_as_reply(message)
      @client.leaders.each do |leader|
        @client.send_message(msg, leader)
      end
      notify_client(message)
    else
      assert false, "Unexpected state / command combination!"
    end
  end

  def notify_client(message)
    @client.notify_client(self, message)
  end
end
end
