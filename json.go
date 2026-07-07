package websocket

import "encoding/json"

// WriteJSON marshals v to JSON and sends it as a text message.
func (c *Conn) WriteJSON(v any) error {
	data, err := json.Marshal(v)
	if err != nil {
		return err
	}
	return c.WriteMessage(TextMessage, data)
}

// ReadJSON reads the next data message and unmarshals it into v. Control frames
// are handled transparently by the underlying read.
func (c *Conn) ReadJSON(v any) error {
	_, data, err := c.ReadMessage()
	if err != nil {
		return err
	}
	return json.Unmarshal(data, v)
}
