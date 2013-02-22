require "socket"

module ToyPaxos
class MessagePump
  # The MessagePump encapsulates the socket connection,
  # and is responsible for feeding messages to its owner

  attr_reader :abort

  def initialize(port)
    @socket = UDPSocket.open
    @socket.bind("localhost", port)
    @abort = false
  end

  def do_abort
    @abort = true
  end

  def send_message(message)
    bytes = Marshal.dump(message)
    @socket.send(bytes, 0, "localhost", message.to)
  end

  def start(&block)
    loop do
      break if @abort
      message = recv_message
      block.call message
    end
  end

private
  def recv_message
    ret = IO.select([@socket], [], [], 1)
    if ret == nil
        return nil
    end
    message, inet_addr = @socket.recvfrom(65535)
    return Marshal.load(message)
  end

end
end
