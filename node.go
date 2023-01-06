package Rabia

func Node(
	n uint32,
	address string,
	addresses []string,
	pipes ...uint16,
) error {
	var log = makeLog(n, 65536)
	var instances = make([]uint64, COUNT)
	for index, pipe := range pipes {
		proposals, reason := TCP(address, int(pipe+1), addresses...)
		if reason != nil {
			return reason
		}
		states, reason := TCP(address, int(pipe+2), addresses...)
		if reason != nil {
			return reason
		}
		votes, reason := TCP(address, int(pipe+3), addresses...)
		if reason != nil {
			return reason
		}

		var slot = uint16(0)
		go func() {
			reason := log.SMR(proposals, states, votes, func() (uint16, uint64) {
				var result uint64
				result, instances = instances[0], instances[1:]
				return slot, result
			}, func(slot uint16, message uint64) {
				print("Ok working ig?")
			})
			if reason != nil {
				print("error")
				print(reason)
			}
		}()
	}

}
