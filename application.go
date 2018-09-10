package app

import (
	"context"
	"fmt"
	"math/rand"
	"net"
	"net/http"
	"net/http/pprof"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/goldenspider/app/config"
	"github.com/goldenspider/app/log"

	etcdv3 "github.com/coreos/etcd/clientv3"
	sd "github.com/coreos/etcd/clientv3/naming"
	grpc_middleware "github.com/grpc-ecosystem/go-grpc-middleware"
	grpc_recovery "github.com/grpc-ecosystem/go-grpc-middleware/recovery"
	grpc_ctxtags "github.com/grpc-ecosystem/go-grpc-middleware/tags"
	grpc_opentracing "github.com/grpc-ecosystem/go-grpc-middleware/tracing/opentracing"
	prometheus "github.com/grpc-ecosystem/go-grpc-prometheus"
	opentracing "github.com/opentracing/opentracing-go"
	zipkin "github.com/openzipkin/zipkin-go-opentracing"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"go.uber.org/zap"
	"golang.org/x/net/trace"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

var (
	envUnetService  = "UNET_SERVICE"
	envUnetCluster  = "UNET_CLUSTER"
	envUnetInstance = "UNET_INSTANCE"
	envUnetHostIP   = "UNET_HOSTIP"

	PrefixServiceDiscover = "/ns"

	MetaRouteBy = "x-ucloud-routeby"

	mgmtInterfaces = map[string]bool{"eth0": true, "net0": true}
)

type (
	ZipkinConfig struct {
		Hosts []string
	}

	GRPCDialConfig struct {
		Timeout        time.Duration
		Block          bool
		Insecure       bool
		WithProxyPort  uint16
		WithRoundRobin bool
		OpenTracing    bool
		Prometheus     bool
		LoggingZap     bool
		MaxMsgSize     int
	}

	GRPCServerConfig struct {
		CtxTags     bool
		OpenTracing bool
		Prometheus  bool
		Recovery    bool
		MaxMsgSize  int
	}

	ApplicationConfig struct {
		ServicePort uint16
		AdminPort   uint16
		LocalIP     string
		Logger      log.LoggerConfig
		Zipkin      ZipkinConfig
		GRPCDial    GRPCDialConfig
		GRPCServer  GRPCServerConfig
	}

	ApplicationConfigurator interface {
		AppConfig() *ApplicationConfig
	}

	Application struct {
		*log.Logger
		config.Name
		ApplicationConfigurator
		Context context.Context
		Etcd    *etcdv3.Client
		Tracer  opentracing.Tracer
		Grpc    *grpc.Server
		Http    *http.Server
		cancel  context.CancelFunc
		config  *config.EtcdConfigService
		admin   *http.Server
		online  *OnlineRegister
	}
)

func (a *ApplicationConfig) AppConfig() *ApplicationConfig { return a }

func (app *Application) Init(cfg ApplicationConfigurator) {
	l, _ := zap.NewDevelopment()
	zap.ReplaceGlobals(l)

	app.Context, app.cancel = context.WithCancel(context.Background())
	app.initName()
	app.initEtcd()
	app.initConfig(app.Context, cfg)
	app.initLogger()
	app.initTracer()
	app.initOnline()

	if app.AppConfig().AdminPort != 0 {
		app.initAdminServer()
	}

	app.Info("The application is initialized.")
}

func (app *Application) Start() error {
	app.Info("starting application...")

	if app.AppConfig().AdminPort != 0 {
		app.startAdminServer()
	}

	if app.Grpc != nil {
		if err := app.startGRPCServer(); err != nil {
			return err
		}
	}

	if app.Http != nil {
		if err := app.startHTTPServer(); err != nil {
			return err
		}
	}

	return nil
}

func (app *Application) Stop() error {
	app.Offline()

	app.cancel()

	if nil != app.admin {
		app.stopAdminServer()
	}

	if nil != app.Grpc {
		app.Grpc.Stop()
		app.Info("GRPC server is stopped.")
	}

	if nil != app.Http {
		ctx, cancel := context.WithTimeout(context.TODO(), 3*time.Second)
		defer cancel()

		if err := app.Http.Shutdown(ctx); err != nil {
			app.Warn("failed to stop HTTP server.", zap.Error(err))
		}
		app.Info("HTTP server is stopped.")
	}

	if nil != app.Etcd {
		if err := app.Etcd.Close(); err != nil {
			app.Warn("failed to stop etcd client.", zap.Error(err))
		}
		app.Logger.Debug("Etcd client is stopped.")
	}

	return nil
}

func (app *Application) IsStopping() bool {
	select {
	case _, ok := <-app.Context.Done():
		return !ok
	default:
		return false
	}
}

func (app *Application) StartConfigWatcher(cfg ApplicationConfigurator, w config.WatchFunc) error {
	app.config.AddWatcher(func(dyn interface{}) {
		cfg := dyn.(ApplicationConfigurator).AppConfig()
		app.onConfigChanged(cfg)
	})
	if nil != w {
		app.config.AddWatcher(w)
	}

	go func() {
		l := app.Named("ConfigWatcher")
		for {
			if e := app.config.Watch(app.Context, cfg); e != nil && !app.IsStopping() {
				l.Warn("failed to watch config", zap.Error(e))
			}
			time.Sleep(1 * time.Second)
		}
		l.Info("Config watcher is exited.")
	}()
	app.Info("Config watcher is started.")
	return nil
}

func (app *Application) Dial(target string, opts ...grpc.DialOption) (*grpc.ClientConn, error) {
	options := make([]grpc.DialOption, 0, 10)
	unary := make([]grpc.UnaryClientInterceptor, 0, 10)
	stream := make([]grpc.StreamClientInterceptor, 0, 10)

	if app.AppConfig().GRPCDial.Block {
		options = append(options, grpc.WithBlock())
	}

	if app.AppConfig().GRPCDial.Insecure {
		options = append(options, grpc.WithInsecure())
	}

	if host, _, e := net.SplitHostPort(target); e != nil || nil == net.ParseIP(host) {
		// If target is IP address, directly dial it
		if 0 != app.AppConfig().GRPCDial.WithProxyPort {
			options = append(options, grpc.WithDialer(app.dialByProxy))
		}

		if app.AppConfig().GRPCDial.WithRoundRobin {
			r := &sd.GRPCResolver{Client: app.Etcd}
			options = append(options, grpc.WithBalancer(grpc.RoundRobin(r)))
		}
	}

	if app.AppConfig().GRPCDial.OpenTracing {
		unary = append(unary, grpc_opentracing.UnaryClientInterceptor(grpc_opentracing.WithTracer(app.Tracer)))
		stream = append(stream, grpc_opentracing.StreamClientInterceptor(grpc_opentracing.WithTracer(app.Tracer)))
	}

	if app.AppConfig().GRPCDial.Prometheus {
		unary = append(unary, prometheus.UnaryClientInterceptor)
		stream = append(stream, prometheus.StreamClientInterceptor)
	}

	if 0 != len(unary) {
		options = append(options, grpc.WithUnaryInterceptor(grpc_middleware.ChainUnaryClient(unary...)))
	}

	if 0 != len(stream) {
		options = append(options, grpc.WithStreamInterceptor(grpc_middleware.ChainStreamClient(stream...)))
	}

	size := app.AppConfig().GRPCDial.MaxMsgSize
	if size > 0 {
		options = append(options, grpc.WithDefaultCallOptions(grpc.MaxCallRecvMsgSize(size), grpc.MaxCallSendMsgSize(size)))
	}

	for _, opt := range opts {
		options = append(options, opt)
	}

	ctx, cancel := context.WithTimeout(context.TODO(), app.AppConfig().GRPCDial.Timeout*time.Second)
	defer cancel()
	return grpc.DialContext(ctx, target, options...)
}

func (app *Application) InitGRPCServer(opts ...grpc.ServerOption) {
	options := make([]grpc.ServerOption, 0, 10)
	unary := make([]grpc.UnaryServerInterceptor, 0, 10)
	stream := make([]grpc.StreamServerInterceptor, 0, 10)

	if app.AppConfig().GRPCServer.CtxTags {
		unary = append(unary, grpc_ctxtags.UnaryServerInterceptor())
		stream = append(stream, grpc_ctxtags.StreamServerInterceptor())
	}

	if app.AppConfig().GRPCServer.OpenTracing {
		unary = append(unary, grpc_opentracing.UnaryServerInterceptor(grpc_opentracing.WithTracer(app.Tracer)))
		stream = append(stream, grpc_opentracing.StreamServerInterceptor(grpc_opentracing.WithTracer(app.Tracer)))
	}

	if app.AppConfig().GRPCServer.Prometheus {
		unary = append(unary, prometheus.UnaryServerInterceptor)
		stream = append(stream, prometheus.StreamServerInterceptor)
	}

	if app.AppConfig().GRPCServer.Recovery {
		unary = append(unary, grpc_recovery.UnaryServerInterceptor(grpc_recovery.WithRecoveryHandler(recoveryHandler)))
		stream = append(stream, grpc_recovery.StreamServerInterceptor(grpc_recovery.WithRecoveryHandler(recoveryHandler)))
	}

	if 0 != len(unary) {
		options = append(options, grpc.UnaryInterceptor(grpc_middleware.ChainUnaryServer(unary...)))
	}

	if 0 != len(stream) {
		options = append(options, grpc.StreamInterceptor(grpc_middleware.ChainStreamServer(stream...)))
	}

	size := app.AppConfig().GRPCServer.MaxMsgSize
	if size > 0 {
		options = append(options, grpc.MaxRecvMsgSize(size), grpc.MaxSendMsgSize(size))
	}

	for _, opt := range opts {
		options = append(options, opt)
	}

	app.Grpc = grpc.NewServer(options...)
}

func (app *Application) NewRouteByContext(ctx context.Context, routes map[string]string) context.Context {
	return NewRouteByContext(ctx, routes)
}

func NewRouteByContext(ctx context.Context, routes map[string]string) context.Context {
	attrs := make([]string, 0, len(routes))
	for k, v := range routes {
		attrs = append(attrs, fmt.Sprintf("%s=%s", k, v))
	}
	sort.Strings(attrs)

	value := strings.Join(attrs, "; ")

	return metadata.NewOutgoingContext(ctx, metadata.Pairs(MetaRouteBy, value))
}

func (app *Application) startGRPCServer() error {
	if app.Grpc == nil {
		return fmt.Errorf("GRPC server isn't initialized")
	}

	addr := app.ServiceAddress()
	lis, err := net.Listen("tcp", addr)
	if err != nil {
		return fmt.Errorf("failed to listen GRPC server at %s. err = %v", addr, err)
	}
	app.Info("GRPC server is listen.", zap.String("Listen", addr))

	go func() {
		l := app.L().Named("GRPCServer")
		if e := app.Grpc.Serve(lis); e != nil && !app.IsStopping() {
			l.Panic("The GRPC server has error.", zap.Error(e))
		}
		l.Info("The GRPC server is exited.")
	}()
	app.Info("GRPC server is started.")

	if err = app.Online(); err != nil {
		return fmt.Errorf("failed to online. error = %v", err)
	}
	return nil
}

func (app *Application) InitHttpServer(handler http.Handler) {
	app.Http.Handler = handler
}

func (app *Application) startHTTPServer() error {
	if app.Http == nil {
		return fmt.Errorf("HTTP server isn't initialized")
	}

	app.Http.Addr = app.ServiceAddress()
	go func() {
		l := app.Named("HttpServer")
		l.Infof("The HTTP server is listen at %s", app.ServiceAddress())
		if e := app.Http.ListenAndServe(); e != nil && !app.IsStopping() {
			l.Panic("failed to listen and serve http server.", zap.Error(e))
		}
		l.Info("The HTTP server is exited.")
	}()
	app.Info("HTTP server is started.")

	if err := app.Online(); err != nil {
		return fmt.Errorf("failed to online. error = %v", err)
	}
	return nil
}

func (app *Application) ServiceAddress() string {
	return fmt.Sprintf("%s:%d", app.AppConfig().LocalIP, app.AppConfig().ServicePort)
}

func (app *Application) Online() error {
	return app.online.Online()
}

func (app *Application) Offline() error {
	return app.online.Offline()
}

func (app *Application) IsMaster(ctx context.Context) (bool, error) {
	return app.online.IsMaster(ctx)
}

func (app *Application) initName() {
	hostname, _ := os.Hostname()
	name := config.Name{
		Instance: hostname,
		Cluster:  hostname,
		Service:  filepath.Base(os.Args[0]),
	}

	service := os.Getenv(envUnetService)
	if "" != service {
		name.Service = service
		zap.S().Infof("load env. %s = %v", envUnetService, service)
	}

	cluster := os.Getenv(envUnetCluster)
	if "" != cluster {
		name.Cluster = cluster
		zap.S().Infof("load env. %s = %v", envUnetCluster, cluster)
	}

	instance := os.Getenv(envUnetInstance)
	if "" != instance {
		name.Instance = instance
		zap.S().Infof("load env. %s = %v", envUnetInstance, instance)
	}

	zap.S().Infof("name = %s.%s.%s", name.Service, name.Cluster, name.Instance)
	app.Name = name
}

func (app *Application) initEtcd() {
	var err error
	etcdCfg := config.GetDefaultEtcdConfig()
	if app.Etcd, err = etcdv3.New(etcdCfg); err != nil {
		zap.L().Panic("failed to init etcd client",
			zap.Error(err),
			zap.String("config", fmt.Sprintf("%#v", etcdCfg)))
	}
}

func (app *Application) initConfig(ctx context.Context, cfg ApplicationConfigurator) {
	app.config = config.NewEtcdConfigService(app.Etcd, app.Name)
	if err := app.config.Fetch(ctx, cfg); err != nil {
		zap.L().Panic("failed to load config from etcd.", zap.Error(err))
	}
	zap.L().Info("Loaded config.", zap.String("config", fmt.Sprintf("%#v", cfg)))

	app.ApplicationConfigurator = cfg
	if app.AppConfig().LocalIP == "" {
		app.AppConfig().LocalIP = findLocalIP()
		zap.S().Infow("Found local IP.", "LocalIP", app.AppConfig().LocalIP)
	}
}

func (app *Application) initLogger() {
	l, err := log.NewLogger(&app.AppConfig().Logger)
	if err != nil {
		zap.L().Panic("failed to new logger.",
			zap.Error(err),
			zap.String("config", fmt.Sprintf("%#v", app.AppConfig().Logger)))
	}
	l.With(
		zap.String("Service", app.Service),
		zap.String("Cluster", app.Cluster),
		zap.String("Instance", app.Instance),
	)
	zap.ReplaceGlobals(l.L())
	app.Logger = l
}

func (app *Application) initTracer() {
	hosts := app.AppConfig().Zipkin.Hosts
	if 0 == len(hosts) {
		app.Warnf("Can't find zipkin servers, just disable opentracing")
		app.AppConfig().GRPCDial.OpenTracing = false
		app.AppConfig().GRPCServer.OpenTracing = false
		return
	}

	url := fmt.Sprintf("http://%s:9411/api/v1/spans", hosts[rand.Intn(len(hosts))])
	collector, err := zipkin.NewHTTPCollector(url)
	if err != nil {
		app.L().Panic("failed to new zipkin HTTP Collector.", zap.Error(err), zap.String("Address", url))
	}
	tracer, err := zipkin.NewTracer(
		zipkin.NewRecorder(collector, false, fmt.Sprintf("%s:%d", app.AppConfig().LocalIP, app.AppConfig().ServicePort), app.Name.Instance),
		zipkin.ClientServerSameSpan(true),
	)
	if err != nil {
		app.L().Panic("failed to zipkin.NewTracer.", zap.Error(err))
	}
	opentracing.InitGlobalTracer(tracer)
	app.Tracer = tracer
	app.Infof("The opentracing client is initlized with %s", url)
}

func (app *Application) initOnline() {
	name := fmt.Sprintf("%s/%s/%s", PrefixServiceDiscover, app.Service, app.Cluster)
	addr := app.ServiceAddress()

	var e error
	app.online, e = newOnlineRegister(app.SugaredLogger, app.Etcd, name, addr, 10)
	if e != nil {
		zap.S().Panic("failed to init online register", zap.Error(e))
	}
}

func (app *Application) initAdminServer() {
	prometheus.EnableClientHandlingTimeHistogram()
	http.Handle("/metrics", promhttp.Handler())
	app.Debug("Admin server is initialized.")

	trace.AuthRequest = func(req *http.Request) (any, sensitive bool) {
		return true, true
	}
	http.Handle("/pprof", pprof.Handler("pprof"))
}

func (app *Application) startAdminServer() error {
	addr := fmt.Sprintf("%s:%d", app.AppConfig().LocalIP, app.AppConfig().AdminPort)
	app.admin = &http.Server{Addr: addr, Handler: nil}
	app.Info("Admin server is listen.", zap.String("Listen", addr))
	go func() {
		l := app.L().Named("AdminServer")
		if e := app.admin.ListenAndServe(); e != nil && !app.IsStopping() {
			l.Panic("failed to listen and serve admin http server.", zap.Error(e))
		}
		l.Info("The admin server is exited.")
	}()
	app.Info("Admin server is started.")
	return nil
}

func (app *Application) stopAdminServer() {
	ctx, cancel := context.WithTimeout(context.TODO(), 3*time.Second)
	defer cancel()

	if err := app.admin.Shutdown(ctx); err != nil {
		app.Warn("failed to stop admin http server.", zap.Error(err))
	}
	app.Info("Admin HTTP server is stopped.")
}

func (app *Application) onConfigChanged(cfg *ApplicationConfig) {
	if err := app.SetLevel(cfg.Logger.Level); err != nil {
		app.Warn("failed to apply dynamic config.", zap.Error(err))
	}
}

func (app *Application) dialByProxy(addr string, timeout time.Duration) (net.Conn, error) {
	proxy := fmt.Sprintf("%s:%d", "127.0.0.1", app.AppConfig().GRPCDial.WithProxyPort)
	app.Infof("dial by %s", proxy)
	return net.DialTimeout("tcp", proxy, timeout)
}

func recoveryHandler(p interface{}) error {
	zap.L().Warn("catch a panic.", zap.Any("error", p), zap.Stack("stack"))
	return status.Errorf(codes.Internal, "%s", p)
}

// 优先从环境变量读取，未找到才尝试从管理网卡获取；返回管理网卡上的第一个IP地址，管理网卡按照系统运维部署的规范hardcode为eth0或者net0
func findLocalIP() string {
	hostIP := os.Getenv(envUnetHostIP)
	if "" != hostIP {
		zap.S().Infof("load LocalIP from env. %s = %v", envUnetHostIP, hostIP)
		return hostIP
	}

	ifaces, err := net.Interfaces()
	if err != nil {
		zap.L().Panic("failed to list interfaces.", zap.Error(err))
	}

	for _, iface := range ifaces {
		if _, ok := mgmtInterfaces[iface.Name]; !ok {
			continue
		}

		addresses, err := iface.Addrs()
		if err != nil {
			zap.S().Panicf("failed to list interface %s addresses.", iface.Name, zap.Error(err))
		}

		for _, addr := range addresses {
			return strings.Split(addr.String(), "/")[0]
		}
	}
	return "127.0.0.1"
}
