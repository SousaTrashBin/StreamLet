package utils.communication;

import utils.application.Message;

public record MessageWithReceiver(Integer receiverId, Message message) {
}
