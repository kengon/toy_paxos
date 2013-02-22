module ToyPaxos
#
# This is a bookkeeping class, which keeps a record of
# all proposals we've seen or undertaken for a given record,
# both on the acceptor and the leader
#
class InstanceRecord
  attr_accessor :value, :accepted
  attr_reader :protocols, :highest_id, :logger

  def initialize(logger)
    @logger = logger
    @protocols = {}
    @highest_id = [-1,-1]
    @value = nil
    @accepted = false
  end

  def add_protocol(protocol)
    @protocols[protocol.proposal_id] = protocol

    if protocol.proposal_id[1] > @highest_id[1] or
       (protocol.proposal_id[1] == @highest_id[1] and
        protocol.proposal_id[0] > @highest_id[0])
      @highest_id = protocol.proposal_id
    end
  end

  def get_protocol(protocol_id)
    @protocols[protocol_id]
  end

  def clean_protocols
    @protocols.each do |pid, protocol|
      if protocol.state == LeaderProtocol::STATE_ACCEPTED
        logger.debug "delete protocol: #{pid}"
        @protocols.delete pid
      end
    end
  end
end
end
