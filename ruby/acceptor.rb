require 'message_pump.rb'
require 'acceptor_protocol.rb'

module ToyPaxos

class Acceptor
  attr_reader :logger, :leaders

  def initialize(logger, port, leaders)
    @port = port
    @leaders = leaders
    @instances = {}
    @msg_pump = MessagePump.new(port)
    @failed = false
    @logger = logger
  end

  def start
    t1 = Thread.new do
      begin
        @msg_pump.start do |message|
          logger.debug("acceptor(#{@port}) receive message=#{message}")
          recv_message(message) if message
        end
        logger.info "thread(message_pump) finish"
      rescue => e
        logger.error e
        logger.error e.backtrace
      end
    end
    #t1.join
  end

  def stop
    @msg_pump.do_abort
  end

  def fail
    @failed = true
  end

  def recover
    @failed = false
  end

  def send_message(message, dest = nil)
    message.to = dest if dest
    @msg_pump.send_message(message)
  end

  def notify_client(protocol, message)
    if protocol.state == AcceptorProtocol::STATE_PROPOSAL_ACCEPTED:
      @instances[protocol.instance_id].value = message.value
      #puts "Proposal accepted at client: #{message.value}"
    end
  end

  def get_highest_agreed_proposal(instance)
    return @instances[instance].highest_id
  end

  def get_instance_value(instance)
    return @instances[instance].value
  end

private
  def recv_message(message)
    raise "message is not nil" unless message

    # Failure means ignored and lost messages
    return if @failed

    if message.command == Message::PROPOSE
      if !@instances.has_key? message.instance_id
        @instances[message.instance_id] = InstanceRecord.new(logger)
      end
      protocol = AcceptorProtocol.new(self)
      protocol.recv_proposal(message)
      @instances[message.instance_id].add_protocol(protocol)
    else
      @instances[message.instance_id].get_protocol(message.proposal_id).
                                      do_transition(message)
    end
  end

end
end
