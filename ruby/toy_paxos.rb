require 'leader.rb'
require 'acceptor.rb'
require 'message.rb'
require 'logger'

logger = Logger.new(STDOUT)
logger.level = Logger::INFO # Logger::DEBUG

numacceptors = 5
acceptor_ports = (64320..(64320+numacceptors-1)).to_a
acceptors = []
acceptor_ports.each do |n|
  acceptors.push ToyPaxos::Acceptor.new(logger, n, [54321,54322])
end
leader1 = ToyPaxos::Leader.new(logger, 54321, [54322], acceptor_ports)
leader2 = ToyPaxos::Leader.new(logger, 54322, [54321], acceptor_ports)
leader1.start
leader1.set_primary(true)
leader2.set_primary(true)
leader2.start
acceptors.each do |c|
  c.start
end

acceptors[0].fail
acceptors[1].fail
#acceptors[2].fail

# Send some proposals through to test
s = UDPSocket.open
start_time = Time.now
n = 1000
n.times do |i|
  m = ToyPaxos::Message.new(ToyPaxos::Message::EXT_PROPOSE)
  m.value = 0 + i
  m.to = 54322
  bytes = Marshal.dump(m)
  len = s.send(bytes, 0, "localhost", m.to)
end

while leader2.get_num_accepted < (n - 1):
  puts "Sleeping for 1s -- accepted: #{leader2.get_num_accepted}"
  sleep(1)
end
end_time = Time.now

puts "Sleeping for 10s"
sleep(10)
puts "Stopping leaders"
leader1.stop
leader2.stop
puts "Stopping acceptors"
acceptors.each do |c|
  c.stop
end

puts "Leader 1 history: #{leader1.get_history.join(",")}"
puts "Leader 2 history: #{leader2.get_history.join(",")}"
puts end_time - start_time
