package message

import (
	"fmt"
	"hash/crc32"
	"reflect"

	"github.com/TangSengDaoDao/TangSengDaoDaoServerLib/config"
	"github.com/TangSengDaoDao/TangSengDaoDaoServerLib/pkg/db"
	"github.com/TangSengDaoDao/TangSengDaoDaoServerLib/pkg/util"
	"github.com/gocraft/dbr/v2"
)

// DB DB
type DB struct {
	session *dbr.Session
	ctx     *config.Context
}

// NewDB NewDB
func NewDB(ctx *config.Context) *DB {
	return &DB{
		session: ctx.DB(),
		ctx:     ctx,
	}
}

// InsertTx 添加消息
// func (d *DB) InsertTx(m *Model, tx *dbr.Tx) error {
// 	_, err := tx.InsertInto("message").Columns(util.AttrToUnderscore(m)...).Record(m).Exec()
// 	return err
// }

func (d *DB) queryMessageWithKeys(key string) ([]*messageModelSimple, error) {
	var models []*messageModelSimple

	// 构建 SQL 查询字符串
	query := `
		SELECT message_id, message_seq, client_msg_no, from_uid, channel_id, channel_type, timestamp, payload as content  FROM message WHERE payload LIKE ?
		UNION 
		SELECT message_id, message_seq, client_msg_no, from_uid, channel_id, channel_type, timestamp, payload  as content  FROM message1 WHERE payload LIKE ?
		UNION 
		SELECT message_id, message_seq, client_msg_no, from_uid, channel_id, channel_type, timestamp, payload  as content  FROM message2 WHERE payload LIKE ?
		UNION 
		SELECT message_id, message_seq, client_msg_no, from_uid, channel_id, channel_type, timestamp, payload  as content  FROM message3 WHERE payload LIKE ?
		UNION 
		SELECT message_id, message_seq, client_msg_no, from_uid, channel_id, channel_type, timestamp, payload  as content FROM message4 WHERE payload LIKE ? 
		ORDER BY timestamp DESC
		LIMIT 50`

	// 执行查询
	rows, err := d.session.Query(query, "%"+key+"%", "%"+key+"%", "%"+key+"%", "%"+key+"%", "%"+key+"%")
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	// 遍历结果集
	for rows.Next() {
		// 创建一个新的 messageModel 实例
		m := &messageModelSimple{}

		// 使用 Scan 方法将当前行数据读取到结构体中
		err := rows.Scan(
			&m.MessageID,
			&m.MessageSeq,
			&m.ClientMsgNo,
			&m.FromUID,
			&m.ChannelID,
			&m.ChannelType,
			&m.Timestamp,
			&m.Payload,
		)
		if err != nil {
			return nil, err
		}

		// 将读取到的结构体添加到结果切片中
		models = append(models, m)
	}

	// 检查查询是否有错误
	if err = rows.Err(); err != nil {
		return nil, err
	}

	return models, nil
}

func (d *DB) queryMessageWithMessageID(channelID string, channelType uint8, messageID string) (*messageModel, error) {
	var m *messageModel
	_, err := d.session.Select("*").From(d.getTable(channelID)).Where("message_id=?", messageID).Load(&m)
	return m, err
}

func (d *DB) queryMessagesWithMessageIDs(channelID string, channelType uint8, messageIDs []string) ([]*messageModel, error) {
	if len(messageIDs) <= 0 {
		return nil, nil
	}
	var models []*messageModel
	_, err := d.session.Select("*").From(d.getTable(channelID)).Where("message_id in ?", messageIDs).Load(&models)
	return models, err
}

func (d *DB) queryMaxMessageSeq(channelID string, channelType uint8) (uint32, error) {
	var maxMessageSeq uint32
	err := d.session.Select("IFNULL(max(message_seq),0)").From(d.getTable(channelID)).Where("channel_id=? and channel_type=?", channelID, channelType).LoadOne(&maxMessageSeq)
	return maxMessageSeq, err
}

func (d *DB) queryMessagesWithChannelClientMsgNo(channelID string, channelType uint8, clientMsgNo string) ([]*messageModel, error) {
	var models []*messageModel
	_, err := d.session.Select("*").From(d.getTable(channelID)).Where("channel_id=? and channel_type=? and client_msg_no=?", channelID, channelType, clientMsgNo).Load(&models)
	return models, err
}
func (d *DB) queryProhibitWordsWithVersion(version int64) ([]*ProhibitWordModel, error) {
	var list []*ProhibitWordModel
	_, err := d.session.Select("*").From("prohibit_words").Where("`version` > ?", version).Load(&list)
	return list, err
}

// 新增消息
func (d *DB) insertMessage(m *messageModel) error {
	_, err := d.session.InsertInto(d.getTable(m.ChannelID)).Columns(util.AttrToUnderscore(m)...).Record(m).Exec()
	return err
}

// 通过频道ID获取表
func (d *DB) getTable(channelID string) string {
	tableIndex := crc32.ChecksumIEEE([]byte(channelID)) % uint32(d.ctx.GetConfig().TablePartitionConfig.MessageTableCount)
	if tableIndex == 0 {
		return "message"
	}
	return fmt.Sprintf("message%d", tableIndex)
}

// ProhibitWordModel 违禁词model
type ProhibitWordModel struct {
	Content   string
	IsDeleted int
	Version   int64
	db.BaseModel
}

// mapToStruct 将数据库查询结果的 map 转换为结构体
func mapToStruct(m map[string]interface{}, result interface{}) error {
	// 获取目标结构体的值和类型
	val := reflect.ValueOf(result).Elem()
	typ := val.Type()

	for i := 0; i < val.NumField(); i++ {
		field := val.Field(i)
		fieldType := typ.Field(i)
		columnName := fieldType.Name

		// 获取 map 中的值
		if value, ok := m[columnName]; ok {
			// 使用反射将值设置到结构体字段中
			if field.CanSet() {
				switch field.Kind() {
				case reflect.Int64:
					field.SetInt(value.(int64))
				case reflect.Uint32:
					switch v := value.(type) {
					case int64:
						field.SetUint(uint64(v))
					case uint32:
						field.SetUint(uint64(v))
					}
				case reflect.String:
					field.SetString(value.(string))
				case reflect.Uint8:
					field.SetUint(uint64(value.(int64)))
				case reflect.Int:
					field.SetInt(value.(int64))
				case reflect.Slice:
					// 假设字段是 []byte
					field.SetBytes(value.([]byte))
				}
			}
		}
	}
	return nil
}

// Model 消息model
type messageModel struct {
	MessageID   int64
	MessageSeq  uint32
	ClientMsgNo string
	Header      string
	Setting     uint8
	FromUID     string
	ChannelID   string
	ChannelType uint8
	Timestamp   int64
	// Type        int
	Payload   []byte
	IsDeleted int
	Signal    int
	Expire    uint32
	db.BaseModel
}

type messageModelSimple struct {
	MessageID   int64
	MessageSeq  uint32
	ClientMsgNo string
	FromUID     string
	ChannelID   string
	ChannelType uint8
	Timestamp   int64
	Payload     string
}
