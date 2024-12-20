package webhook

import (
	"bytes"
	"encoding/json"
	"fmt"
	"github.com/TangSengDaoDao/TangSengDaoDaoServerLib/config"
	"github.com/TangSengDaoDao/TangSengDaoDaoServerLib/pkg/log"
	"go.uber.org/zap"
	"net/http"
	"strings"
	"time"
)

const (
	ECHOOO_PUSH_UID   = "tsdd:echooo:push_uid:"
	ECHOOO_FEISHU_UID = "tsdd:echooo:feishu_uid:"
)

type EchoooPush struct {
	serverAddresses string
	log.Log
	ctx *config.Context
}

func NewEchoooPush(serverAddresses string, ctx *config.Context) *EchoooPush {

	return &EchoooPush{
		ctx:             ctx,
		Log:             log.NewTLog("EchoooPush"),
		serverAddresses: serverAddresses,
	}
}

// Push 推送
func (m *EchoooPush) Push(uid string, content string) error {
	log.Info("EchoooPush serverAddresses", zap.String("serverAddresses", m.serverAddresses), zap.String("im_content", content))

	key := fmt.Sprintf("%s%s", ECHOOO_PUSH_UID, uid)
	result, err := m.ctx.GetRedisConn().GetString(key)
	if err != nil {
		m.Info("pushToEchoooApi to get cache key error")
		return err
	}

	if len(result) > 0 {
		m.Info("uid " + uid + " has push in five minutes. ")
		return nil
	}

	if len(m.serverAddresses) > 0 {
		servers := strings.Split(m.serverAddresses, ",")

		for _, server := range servers {
			m.Info("echooo inner Push server", zap.String("server", server), zap.String("uid", uid))
			params := make(map[string]interface{})
			params["im_content"] = content
			reqParam := SendSinglePushReq{
				UserId:     uid,
				PushType:   3,
				TemplateId: 27,
				Params:     params,
			}
			jsonData, _ := json.Marshal(&reqParam)
			resp, err := http.Post("http://"+server+"/inner/push/sendNotice", "application/json", bytes.NewBuffer(jsonData))
			defer resp.Body.Close()
			if err != nil {
				m.Info("Error reading response body:", zap.Error(err))
				continue
			} else {
				m.ctx.GetRedisConn().SetAndExpire(key, "1", time.Minute*5)
				m.Info("echooo inner Push success server", zap.String("server", server), zap.String("uid", uid))
				break
			}

		}
	}
	return nil
}

type SendSinglePushReq struct {
	UserId     string                 `json:"userId"`
	DeviceId   string                 `json:"deviceId"`
	Lang       string                 `json:"lang"`
	PushType   int                    `json:"pushType"`
	TemplateId int                    `json:"templateId"`
	Params     map[string]interface{} `json:"params"`
}
