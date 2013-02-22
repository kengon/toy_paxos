module ToyPaxos
class Message
  ACCEPTOR_AGREE = 0
  ACCEPTOR_ACCEPT = 1
  ACCEPTOR_REJECT = 2
  ACCEPTOR_UNACCEPT = 3
  ACCEPT = 4
  PROPOSE = 5
  EXT_PROPOSE = 6
  HEARTBEAT = 7

  attr_accessor :to, :source, :command, :value, :proposal_id, :instance_id,
                :sequence

  def initialize(command)
    @command = command
    @proposal_id = nil
    @instance_id = nil
    @to = nil
    @source = nil
    @value = nil
    @sequence = nil
  end

  def copy_as_reply(message)
    # ignore command
    # ignore sequence ?
    @proposal_id = message.proposal_id
    @instance_id = message.instance_id
    @to = message.source
    @source = message.to
    @value = message.value
  end
end
end
