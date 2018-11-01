using Opc.Ua;
using Opc.Ua.Client;
using Opc.Ua.Configuration;
using System;
using System.Linq;
using System.Collections.Generic;
using System.IO;
using System.Reflection;

namespace Opc.Ua.UaHelper
{
    public class OpcUaClient
    {
        ApplicationConfiguration m_configuration;
        Session m_session;
        SessionReconnectHandler m_reconnectHandler;
        Dictionary<string, Subscription> dic_subscriptions = new Dictionary<string, Subscription>();
        bool m_IsConnected = false;
        public OpcUaClient()
        {
            #region 初始化
            var application = new ApplicationInstance();
            application.ApplicationType = ApplicationType.Client;
            application.ConfigSectionName = Path.GetFileName(Assembly.GetExecutingAssembly().Location);

            var certificateValidator = new CertificateValidator();
            certificateValidator.CertificateValidation += new CertificateValidationEventHandler(CertificateValidator_CertificateValidation);
            SecurityConfiguration securityConfiguration = new SecurityConfiguration
            {
                AutoAcceptUntrustedCertificates = true,
                //RejectSHA1SignedCertificates = false,
                //MinimumCertificateKeySize = 1024,
            };
            certificateValidator.Update(securityConfiguration);

            application.ApplicationConfiguration = new ApplicationConfiguration
            {
                ApplicationName = application.ConfigSectionName,
                ApplicationType = ApplicationType.Client,
                CertificateValidator = certificateValidator,
                ServerConfiguration = new ServerConfiguration
                {
                    MaxSubscriptionCount = 100000,
                    MaxMessageQueueSize = 1000000,
                    MaxNotificationQueueSize = 1000000,
                    MaxPublishRequestCount = 10000000,
                },

                SecurityConfiguration = new SecurityConfiguration
                {
                    AutoAcceptUntrustedCertificates = true,
                    //RejectSHA1SignedCertificates = false,
                    //MinimumCertificateKeySize = 1024,
                },

                TransportQuotas = new TransportQuotas
                {
                    OperationTimeout = 6000000,
                    MaxStringLength = int.MaxValue,
                    MaxByteStringLength = int.MaxValue,
                    MaxArrayLength = 65535,
                    MaxMessageSize = 419430400,
                    MaxBufferSize = 65535,
                    ChannelLifetime = -1,
                    SecurityTokenLifetime = -1
                },
                ClientConfiguration = new ClientConfiguration
                {
                    DefaultSessionTimeout = -1,
                    MinSubscriptionLifetime = -1,
                },
                DisableHiResClock = true
            };

            m_configuration = application.ApplicationConfiguration;

            #endregion
        }


        ~OpcUaClient()
        {
            Disconnect();
        }

        #region 服务连接
        public void Connect(string Url)
        {
            if (string.IsNullOrEmpty(Url)) throw new ArgumentNullException("UaServer");

            if (m_configuration == null) throw new ArgumentNullException("m_configuration");

            // select the best endpoint.
            EndpointDescription endpointDescription = SelectEndpoint(Url, false);

            EndpointConfiguration endpointConfiguration = EndpointConfiguration.Create(m_configuration);
            ConfiguredEndpoint endpoint = new ConfiguredEndpoint(null, endpointDescription, endpointConfiguration);

            m_session = Session.Create(
                m_configuration,
                endpoint,
                false,
                false,
                m_configuration.ApplicationName,
                60000,
                new UserIdentity(new AnonymousIdentityToken()),
                new string[] { });

            // set up keep alive callback.
            m_session.KeepAlive += new KeepAliveEventHandler(Session_KeepAlive);

            m_IsConnected = true;
        }


        public void Disconnect()
        {
            // stop any reconnect operation.
            if (m_reconnectHandler != null)
            {
                m_reconnectHandler.Dispose();
                m_reconnectHandler = null;
            }

            // disconnect any existing session.
            if (m_session != null)
            {
                m_session.Close(10000);
                m_session = null;
            }
            m_IsConnected = false;
        }
        #endregion


        #region 数值读写
        public string ReadNode(string NodeId)
        {
            var result = ReadNode(NodeId, Attributes.Value);
            if (result != null && DataValue.IsGood(result))
            {
                return result.Value.ToString();
            }
            return null;
        }

        DataValue ReadNode(string NodeId, uint AttributeId)
        {
            DataValueCollection results = null;
            DiagnosticInfoCollection diagnosticInfos = new DiagnosticInfoCollection();

            ReadValueIdCollection nodesToRead = new ReadValueIdCollection
            {
                new ReadValueId()
                {
                    NodeId = NodeId,
                    AttributeId = AttributeId
                }
            };

            m_session.Read(
          null,
          0,
          TimestampsToReturn.Neither,
          nodesToRead,
          out results,
          out diagnosticInfos);

            //ClientBase.ValidateResponse(results, nodesToRead);
            //ClientBase.ValidateDiagnosticInfos(diagnosticInfos, nodesToRead);

            return results[0];
        }

        public List<string> ReadNodes(List<string> NodeIds)
        {
            var results = ReadNodes(NodeIds, Attributes.Value);
            return results.Select(x => DataValue.IsGood(x) ? x.Value.ToString() : null).ToList();
        }

        List<DataValue> ReadNodes(List<string> NodeIds, uint AttributeId)
        {
            DataValueCollection results = null;
            DiagnosticInfoCollection diagnosticInfos = new DiagnosticInfoCollection();

            ReadValueIdCollection nodesToRead = NodeIds.Select(NodeId => new ReadValueId
            {
                NodeId = NodeId,
                AttributeId = AttributeId
            }).ToArray();

            m_session.Read(
          null,
          0,
          TimestampsToReturn.Neither,
          nodesToRead,
          out results,
          out diagnosticInfos);

            return results.ToList();
        }

        public bool WriteNode(string NodeId, string Value)
        {
            var typeNodeId = ReadNode(NodeId, Attributes.DataType);
            if (!DataValue.IsGood(typeNodeId))
            {
                throw new ApplicationException("数据类型获取失败：" + NodeId);
            }
            BuiltInType builtinType = Opc.Ua.TypeInfo.GetBuiltInType(typeNodeId.Value as NodeId, m_session.TypeTree);

            var nodesToWrite = new WriteValueCollection
            {
                new WriteValue()
                {
                    NodeId = NodeId,
                    AttributeId = Attributes.Value,
                    Value = new DataValue
                    {
                        Value = new Variant(Opc.Ua.TypeInfo.Cast(Value, builtinType)),
                        StatusCode = StatusCodes.Good,
                        ServerTimestamp = DateTime.MinValue,
                        SourceTimestamp = DateTime.MinValue
                    }
                }
            };

            StatusCodeCollection results = new StatusCodeCollection();
            DiagnosticInfoCollection diagnosticInfos = new DiagnosticInfoCollection();

            m_session.Write(
                null,
                nodesToWrite,
                out results,
                out diagnosticInfos);

            //ClientBase.ValidateResponse(results, nodesToWrite);
            //ClientBase.ValidateDiagnosticInfos(diagnosticInfos, nodesToWrite);

            if (StatusCode.IsBad(results[0]))
            {
                throw new ServiceResultException(results[0]);
            }

            return true;
        }

        public bool WriteNodes(List<string> NodeIds, List<string> Values)
        {
            var typeNodeIds = ReadNodes(NodeIds, Attributes.DataType);

            for (int i = 0; i < typeNodeIds.Count; i++)
            {
                if (!DataValue.IsGood(typeNodeIds[i]))
                {
                    throw new ApplicationException("数据类型获取失败：" + NodeIds[i]);
                }
            }

            var builtinTypes = typeNodeIds.Select(typeNodeId => Opc.Ua.TypeInfo.GetBuiltInType(typeNodeId.Value as NodeId, m_session.TypeTree)).ToList();

            var nodesToWrite = new WriteValueCollection();
            for (int i = 0; i < NodeIds.Count; i++)
            {
                nodesToWrite.Add(new WriteValue()
                {
                    NodeId = NodeIds[i],
                    AttributeId = Attributes.Value,
                    Value = new DataValue
                    {
                        Value = new Variant(Opc.Ua.TypeInfo.Cast(Values[i], builtinTypes[i])),
                        StatusCode = StatusCodes.Good,
                        ServerTimestamp = DateTime.MinValue,
                        SourceTimestamp = DateTime.MinValue
                    }
                });
            }

            StatusCodeCollection results = new StatusCodeCollection();
            DiagnosticInfoCollection diagnosticInfos = new DiagnosticInfoCollection();

            m_session.Write(
                null,
                nodesToWrite,
                out results,
                out diagnosticInfos);

            //ClientBase.ValidateResponse(results, nodesToWrite);
            //ClientBase.ValidateDiagnosticInfos(diagnosticInfos, nodesToWrite);

            results.ForEach(x =>
            {
                if (StatusCode.IsBad(x))
                {
                    throw new ServiceResultException(x);
                }
            });

            return true;
        }
        #endregion

        #region 断线重连
        void Session_KeepAlive(Session session, KeepAliveEventArgs e)
        {

            // check for events from discarded sessions.
            if (!Object.ReferenceEquals(session, m_session))
            {
                return;
            }

            if (ServiceResult.IsBad(e.Status))
            {
                m_IsConnected = false;

                m_reconnectHandler = new SessionReconnectHandler();
                m_reconnectHandler.BeginReconnect(m_session, 10000, Server_ReconnectComplete);
            }
        }

        /// <summary>
        /// Handles a reconnect event complete from the reconnect handler.
        /// </summary>
        void Server_ReconnectComplete(object sender, EventArgs e)
        {
            // ignore callbacks from discarded objects.
            if (!Object.ReferenceEquals(sender, m_reconnectHandler))
            {
                return;
            }


            m_session = m_reconnectHandler.Session;
            m_reconnectHandler.Dispose();
            m_reconnectHandler = null;

            m_IsConnected = true;
        }
        #endregion

        #region 证书处理
        /// <summary>
        /// 证书不授信时回调函数
        /// </summary>
        /// <param name="validator"></param>
        /// <param name="e"></param>
        void CertificateValidator_CertificateValidation(CertificateValidator validator, CertificateValidationEventArgs eventArgs)
        {
            if (ServiceResult.IsGood(eventArgs.Error))
                eventArgs.Accept = true;
            else if (eventArgs.Error.StatusCode.Code == StatusCodes.BadCertificateUntrusted)
                eventArgs.Accept = true;
            else
                throw new Exception(string.Format("Failed to validate certificate with error code {0}: {1}", eventArgs.Error.Code, eventArgs.Error.AdditionalInfo));
        }
        #endregion

        #region 查找服务
        /// <summary>
        /// Finds the endpoint that best matches the current settings.
        /// </summary>
        /// <param name="discoveryUrl">The discovery URL.</param>
        /// <param name="useSecurity">if set to <c>true</c> select an endpoint that uses security.</param>
        /// <returns>The best available endpoint.</returns>
        EndpointDescription SelectEndpoint(string discoveryUrl, bool useSecurity)
        {
            // needs to add the '/discovery' back onto non-UA TCP URLs.
            if (!discoveryUrl.StartsWith(Opc.Ua.Utils.UriSchemeOpcTcp))
            {
                if (!discoveryUrl.EndsWith("/discovery"))
                {
                    discoveryUrl += "/discovery";
                }
            }

            // parse the selected URL.
            Uri uri = new Uri(discoveryUrl);

            // set a short timeout because this is happening in the drop down event.
            EndpointConfiguration configuration = EndpointConfiguration.Create();
            configuration.OperationTimeout = 5000;

            EndpointDescription selectedEndpoint = null;

            // Connect to the server's discovery endpoint and find the available configuration.
            using (DiscoveryClient client = DiscoveryClient.Create(uri, configuration))
            {
                EndpointDescriptionCollection endpoints = client.GetEndpoints(null);

                // select the best endpoint to use based on the selected URL and the UseSecurity checkbox. 
                for (int ii = 0; ii < endpoints.Count; ii++)
                {
                    EndpointDescription endpoint = endpoints[ii];

                    // check for a match on the URL scheme.
                    if (endpoint.EndpointUrl.StartsWith(uri.Scheme))
                    {
                        // check if security was requested.
                        if (useSecurity)
                        {
                            if (endpoint.SecurityMode == MessageSecurityMode.None)
                            {
                                continue;
                            }
                        }
                        else
                        {
                            if (endpoint.SecurityMode != MessageSecurityMode.None)
                            {
                                continue;
                            }
                        }

                        // pick the first available endpoint by default.
                        if (selectedEndpoint == null)
                        {
                            selectedEndpoint = endpoint;
                        }

                        // The security level is a relative measure assigned by the server to the 
                        // endpoints that it returns. Clients should always pick the highest level
                        // unless they have a reason not too.
                        if (endpoint.SecurityLevel > selectedEndpoint.SecurityLevel)
                        {
                            selectedEndpoint = endpoint;
                        }
                    }
                }

                // pick the first available endpoint by default.
                if (selectedEndpoint == null && endpoints.Count > 0)
                {
                    selectedEndpoint = endpoints[0];
                }
            }

            // if a server is behind a firewall it may return URLs that are not accessible to the client.
            // This problem can be avoided by assuming that the domain in the URL used to call 
            // GetEndpoints can be used to access any of the endpoints. This code makes that conversion.
            // Note that the conversion only makes sense if discovery uses the same protocol as the endpoint.

            Uri endpointUrl = Opc.Ua.Utils.ParseUri(selectedEndpoint.EndpointUrl);

            if (endpointUrl != null && endpointUrl.Scheme == uri.Scheme)
            {
                UriBuilder builder = new UriBuilder(endpointUrl);
                builder.Host = uri.DnsSafeHost;
                builder.Port = uri.Port;
                selectedEndpoint.EndpointUrl = builder.ToString();
            }

            // return the selected endpoint.
            return selectedEndpoint;
        }
        #endregion

        #region 监控订阅

        /// <summary>
        /// 新增一个订阅，需要指定订阅的关键字，订阅的tag名，以及回调方法
        /// </summary>
        /// <param name="key">关键字</param>
        /// <param name="tag">tag</param>
        /// <param name="callback">回调方法</param>
        public void AddSubscription(string key, string nodeId, Action<string, MonitoredItem, MonitoredItemNotificationEventArgs> callback)
        {
            AddSubscription(key, new string[] { nodeId }, callback);
        }

        /// <summary>
        /// 新增一批订阅，需要指定订阅的关键字，订阅的tag名数组，以及回调方法
        /// </summary>
        /// <param name="key">关键字</param>
        /// <param name="tags">节点名称数组</param>
        /// <param name="callback">回调方法</param>
        public void AddSubscription(string key, string[] nodeIds, Action<string, MonitoredItem, MonitoredItemNotificationEventArgs> callback)
        {
            Subscription m_subscription = new Subscription(m_session.DefaultSubscription);

            m_subscription.PublishingEnabled = true;
            m_subscription.PublishingInterval = 100;
            m_subscription.KeepAliveCount = 1000;
            m_subscription.LifetimeCount = 1000;
            m_subscription.MaxNotificationsPerPublish = 1000;
            m_subscription.Priority = 100;
            m_subscription.DisplayName = key;


            for (int i = 0; i < nodeIds.Length; i++)
            {
                var item = new MonitoredItem
                {
                    StartNodeId = new NodeId(nodeIds[i]),
                    AttributeId = Attributes.Value,
                    DisplayName = nodeIds[i],
                    SamplingInterval = 100,
                };
                item.Notification += (MonitoredItem monitoredItem, MonitoredItemNotificationEventArgs args) =>
                {
                    callback?.Invoke(key, monitoredItem, args);
                };
                m_subscription.AddItem(item);
            }

            m_session.AddSubscription(m_subscription);
            m_subscription.Create();

            if (dic_subscriptions.ContainsKey(key))
            {
                // remove 
                dic_subscriptions[key].Delete(true);
                m_session.RemoveSubscription(dic_subscriptions[key]);
                dic_subscriptions[key].Dispose();
                dic_subscriptions[key] = m_subscription;
            }
            else
            {
                dic_subscriptions.Add(key, m_subscription);
            }
        }

        /// <summary>
        /// 移除订阅消息，如果该订阅消息是批量的，也直接移除
        /// </summary>
        /// <param name="key">订阅关键值</param>
        public void RemoveSubscription(string key)
        {
            if (dic_subscriptions.ContainsKey(key))
            {
                // remove 
                dic_subscriptions[key].Delete(true);
                m_session.RemoveSubscription(dic_subscriptions[key]);
                dic_subscriptions[key].Dispose();
                dic_subscriptions.Remove(key);
            }
        }

        /// <summary>
        /// 移除所有的订阅消息
        /// </summary>
        public void RemoveAllSubscription()
        {
            foreach (var item in dic_subscriptions)
            {
                item.Value.Delete(true);
                m_session.RemoveSubscription(item.Value);
                item.Value.Dispose();
            }
            dic_subscriptions.Clear();
        }


        #endregion

    }
}
