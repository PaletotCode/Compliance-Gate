import React, { useState, useEffect, useRef } from 'react';
import { 
  ShieldCheck, 
  Lock, 
  User, 
  ArrowRight, 
  QrCode, 
  RefreshCw,
  CheckCircle2,
  Moon,
  Sun,
  Shield,
  ChevronRight,
  Smartphone,
  Copy,
  Mail,
  ArrowLeft
} from 'lucide-react';

// --- COMPONENTES AUXILIARES ---

const PrimaryButton = ({ children, onClick, loading, disabled, className = "", delay = "0" }) => {
  const [tilt, setTilt] = useState({ x: 0, y: 0 });
  
  const handleMouseMove = (e) => {
    const rect = e.currentTarget.getBoundingClientRect();
    const x = (e.clientX - rect.left) / rect.width - 0.5;
    const y = (e.clientY - rect.top) / rect.height - 0.5;
    setTilt({ x: x * 8, y: y * -8 });
  };

  return (
    <div 
      className="animate-in fade-in slide-in-from-bottom-4 duration-700 fill-mode-both"
      style={{ animationDelay: `${delay}ms` }}
    >
      <button
        onClick={onClick}
        onMouseMove={handleMouseMove}
        onMouseLeave={() => setTilt({ x: 0, y: 0 })}
        disabled={loading || disabled}
        style={{
          transform: `perspective(500px) rotateX(${tilt.y}deg) rotateY(${tilt.x}deg)`,
          transition: 'transform 0.1s ease-out, background-color 0.2s',
        }}
        className={`w-full h-12 flex items-center justify-center gap-3 rounded-lg font-bold text-white shadow-md active:scale-[0.98] transition-all
          ${loading || disabled ? 'bg-slate-400 cursor-not-allowed' : 'bg-[#00AE9D] hover:shadow-[#00AE9D]/30 hover:brightness-105'} 
          ${className}`}
      >
        {loading ? <RefreshCw className="w-5 h-5 animate-spin" /> : children}
      </button>
    </div>
  );
};

const AuthInput = ({ label, icon: Icon, type = "text", placeholder, value, onChange, delay = "0" }) => (
  <div 
    className="space-y-1.5 group animate-in fade-in slide-in-from-bottom-4 fill-mode-both duration-700"
    style={{ animationDelay: `${delay}ms` }}
  >
    <div className="flex justify-between items-center px-1">
      <label className="text-[10px] font-black text-white uppercase tracking-[1.5px]">
        {label}
      </label>
    </div>
    <div className="relative">
      <div className="absolute left-4 top-1/2 -translate-y-1/2 text-white/60 group-focus-within:text-[#00AE9D] transition-colors">
        <Icon size={16} />
      </div>
      <input
        type={type}
        value={value}
        onChange={onChange}
        placeholder={placeholder}
        className="w-full h-12 pl-11 pr-4 bg-white/30 dark:bg-black/40 border border-white/20 dark:border-white/10 rounded-lg outline-none transition-all 
          focus:bg-white dark:focus:bg-slate-800 focus:border-[#00AE9D] text-slate-800 dark:text-white 
          placeholder:text-slate-100 dark:placeholder:text-white shadow-sm font-semibold"
      />
    </div>
  </div>
);

const MfaCodeInput = ({ onComplete, delay = "0" }) => {
  const [code, setCode] = useState(['', '', '', '', '', '']);
  const inputs = useRef([]);

  const handleChange = (index, value) => {
    if (isNaN(value)) return;
    const newCode = [...code];
    newCode[index] = value.substring(value.length - 1);
    setCode(newCode);

    if (value && index < 5) {
      inputs.current[index + 1].focus();
    }
    
    if (newCode.every(v => v !== '')) {
      onComplete(newCode.join(''));
    }
  };

  const handleKeyDown = (index, e) => {
    if (e.key === 'Backspace' && !code[index] && index > 0) {
      inputs.current[index - 1].focus();
    }
  };

  return (
    <div 
      className="flex justify-between gap-2 animate-in fade-in duration-700 fill-mode-both"
      style={{ animationDelay: `${delay}ms` }}
    >
      {code.map((digit, i) => (
        <input
          key={i}
          ref={el => inputs.current[i] = el}
          type="text"
          maxLength={1}
          value={digit}
          onChange={(e) => handleChange(i, e.target.value)}
          onKeyDown={(e) => handleKeyDown(i, e)}
          className="w-full h-12 text-center text-xl font-black bg-white/30 dark:bg-black/50 border border-white/20 dark:border-white/10 focus:border-[#00AE9D] rounded-lg outline-none text-white transition-all focus:ring-4 focus:ring-[#00AE9D]/10"
          style={{ animationDelay: `${parseInt(delay) + (i * 60)}ms` }}
        />
      ))}
    </div>
  );
};

const SidebarIconGrid = () => {
  const icons = Array.from({ length: 48 });
  return (
    <div className="absolute inset-0 grid grid-cols-6 gap-8 p-10 pointer-events-none opacity-[0.1] dark:opacity-[0.2]">
      {icons.map((_, i) => (
        <div key={i} className="flex items-center justify-center text-white/40 dark:text-[#00AE9D]">
          {i % 3 === 0 ? <Shield size={28} /> : i % 3 === 1 ? <Lock size={28} /> : <ShieldCheck size={28} />}
        </div>
      ))}
    </div>
  );
};

const BackgroundHero = () => {
  return (
    <div className="fixed inset-0 z-0 bg-black">
      <img 
        src="https://images.unsplash.com/photo-1497366216548-37526070297c?auto=format&fit=crop&q=80&w=1920" 
        alt="Sicoob Corporate Background"
        referrerPolicy="no-referrer"
        className="w-full h-full object-cover brightness-[0.6] contrast-125 animate-in fade-in zoom-in-105 duration-[2000ms]"
      />
      <div className="absolute inset-0 bg-gradient-to-r from-black/20 via-transparent to-transparent dark:from-black/40" />
      <div className="absolute inset-0 bg-black/10 dark:bg-black/20" />
    </div>
  );
};

// --- APP PRINCIPAL ---

export default function App() {
  const [theme, setTheme] = useState('light');
  const [step, setStep] = useState('login'); 
  const [loading, setLoading] = useState(false);
  const [mousePos, setMousePos] = useState({ x: 0, y: 0 });
  const containerRef = useRef(null);

  const handleMouseMove = (e) => {
    if (!containerRef.current) return;
    const rect = containerRef.current.getBoundingClientRect();
    setMousePos({ x: e.clientX - rect.left, y: e.clientY - rect.top });
  };

  const handleLogin = () => {
    setLoading(true);
    setTimeout(() => {
      setLoading(false);
      setStep('setup');
    }, 1200);
  };

  const handleForgotPassword = () => {
    setLoading(true);
    setTimeout(() => {
      setLoading(false);
      setStep('login');
    }, 1500);
  };

  const handleSetupComplete = () => {
    setLoading(true);
    setTimeout(() => {
      setLoading(false);
      setStep('mfa');
    }, 1200);
  };

  const handleMfaComplete = () => {
    setLoading(true);
    setTimeout(() => {
      setLoading(false);
      setStep('success');
    }, 1800);
  };

  return (
    <div 
      ref={containerRef}
      onMouseMove={handleMouseMove}
      className={`${theme} relative flex min-h-screen font-sans selection:bg-[#00AE9D]/30 transition-colors duration-500 overflow-hidden`}
      style={{
        '--mouse-x': `${mousePos.x}px`,
        '--mouse-y': `${mousePos.y}px`
      }}
    >
      <BackgroundHero />

      {/* Lanterna Spotlight */}
      <div 
        className="fixed inset-0 pointer-events-none z-10 transition-opacity duration-300 opacity-100"
        style={{
          background: `radial-gradient(600px circle at var(--mouse-x) var(--mouse-y), rgba(0, 174, 157, 0.12), transparent 80%)`
        }}
      />

      <div className="relative z-20 flex w-full min-h-screen">
        
        {/* SIDEBAR COM LIQUID GLASS REAL */}
        <aside className="relative w-full md:w-[540px] flex flex-col justify-between p-10 md:p-14 
          bg-white/10 dark:bg-black/40 backdrop-blur-[35px] backdrop-saturate-[2.5]
          border-r border-white/20 dark:border-white/5 
          shadow-[50px_0_100px_-20px_rgba(0,0,0,0.3)] transition-all duration-700">
          
          <div 
            className="absolute inset-0 pointer-events-none overflow-hidden"
            style={{
              maskImage: `radial-gradient(400px circle at var(--mouse-x) var(--mouse-y), black 0%, transparent 100%)`,
              WebkitMaskImage: `radial-gradient(400px circle at var(--mouse-x) var(--mouse-y), black 0%, transparent 100%)`
            }}
          >
            <SidebarIconGrid />
          </div>

          {/* Branding */}
          <div className="flex items-center justify-between relative z-10 animate-in fade-in slide-in-from-top-6 duration-1000">
            <div className="flex flex-col group cursor-default">
              <span className="font-black text-2xl tracking-[-0.05em] text-white uppercase leading-none">
                SICOOB
              </span>
              <span className="text-[10px] font-black text-[#00AE9D] tracking-[0.5em] uppercase mt-1.5">
                Compliance Gate
              </span>
            </div>
            <button 
              onClick={() => setTheme(t => t === 'light' ? 'dark' : 'light')}
              className="w-10 h-10 flex items-center justify-center rounded-lg bg-white/40 dark:bg-white/5 text-white border border-white/20 dark:border-white/10 hover:text-[#00AE9D] transition-all shadow-sm active:scale-90"
            >
              {theme === 'light' ? <Moon size={18} /> : <Sun size={18} />}
            </button>
          </div>

          <div className="flex-1 flex items-center justify-center py-10 relative z-10">
            
            {step === 'login' && (
              <div key="login" className="w-full space-y-10 animate-in fade-in slide-in-from-bottom-8 duration-1000">
                <div className="flex items-center gap-5">
                  <div className="w-1.5 h-12 bg-[#00AE9D] rounded-full shadow-[0_0_15px_rgba(0,174,157,0.5)]" />
                  <h1 className="text-3xl font-black text-white tracking-tight">
                    Bem-vindo de volta.
                  </h1>
                </div>

                <div className="space-y-4">
                  <AuthInput label="Identidade Corporativa" icon={User} placeholder="nome.sobrenome" delay={200} />
                  <AuthInput label="Senha de Rede" icon={Lock} type="password" placeholder="••••••••" delay={400} />
                </div>

                <PrimaryButton loading={loading} onClick={handleLogin} delay={600}>
                  EFETUAR LOGIN <ArrowRight size={18} />
                </PrimaryButton>

                <div className="flex items-center justify-center gap-4 pt-2 text-[9px] font-black text-white/70 dark:text-white/60 uppercase tracking-widest animate-in fade-in duration-1000" style={{ animationDelay: '800ms' }}>
                  <button onClick={() => setStep('forgot-password')} className="hover:text-[#00AE9D] transition-colors">Esqueci minha senha</button>
                  <span className="w-1 h-1 rounded-full bg-white/30 dark:bg-white/20" />
                  <button className="hover:text-[#00AE9D] transition-colors">Suporte TI</button>
                </div>
              </div>
            )}

            {step === 'forgot-password' && (
              <div key="forgot-password" className="w-full space-y-10 animate-in fade-in slide-in-from-bottom-8 duration-1000">
                <div className="flex items-center gap-5">
                  <button onClick={() => setStep('login')} className="p-2 -ml-2 text-white hover:text-[#00AE9D] transition-colors">
                    <ArrowLeft size={24} />
                  </button>
                  <h1 className="text-3xl font-black text-white tracking-tight">
                    Recuperar Acesso
                  </h1>
                </div>

                <div className="space-y-4">
                  <p className="text-xs text-white/80 font-medium px-1 leading-relaxed">
                    Insira seu e-mail corporativo ou CPF. Enviaremos as instruções de redefinição para o seu dispositivo vinculado.
                  </p>
                  <AuthInput label="Identificação" icon={Mail} placeholder="nome@sicoob.com.br" delay={200} />
                </div>

                <PrimaryButton loading={loading} onClick={handleForgotPassword} delay={400}>
                  SOLICITAR NOVA SENHA <ArrowRight size={18} />
                </PrimaryButton>

                <p className="text-center text-[9px] font-black text-white/50 uppercase tracking-widest">
                  Protegido por Compliance Gate
                </p>
              </div>
            )}

            {step === 'setup' && (
              <div key="setup" className="w-full space-y-8 animate-in fade-in slide-in-from-bottom-8 duration-1000">
                <div className="text-center space-y-2">
                  <h2 className="text-2xl font-black text-white">Vincular Dispositivo</h2>
                  <p className="text-white/80 text-xs font-medium max-w-[320px] mx-auto">
                    Escaneie o código abaixo com o seu <b>Microsoft Authenticator</b>.
                  </p>
                </div>

                <div className="relative group mx-auto w-44 h-44 p-3 bg-white/95 dark:bg-white/10 backdrop-blur-md rounded-lg border-2 border-[#00AE9D]/30 shadow-xl flex items-center justify-center animate-in zoom-in duration-700">
                  <div className="absolute inset-0 opacity-10 bg-[radial-gradient(#00AE9D_1px,transparent_1px)] [background-size:8px_8px] rounded-lg" />
                  <QrCode size={100} className="text-[#00AE9D] relative z-10" />
                  <div className="absolute -top-2 -right-2 bg-[#00AE9D] text-white p-1 rounded-md shadow-lg">
                    <Smartphone size={14} />
                  </div>
                </div>

                <div className="space-y-4">
                  <div className="p-3 bg-white/50 dark:bg-white/5 border border-white/20 rounded-lg flex items-center justify-between group">
                    <div className="flex flex-col">
                      <span className="text-[8px] font-black text-white/40 uppercase tracking-widest">Chave Secreta</span>
                      <span className="text-xs font-mono font-bold text-white tracking-widest uppercase">SICOOB-G8-F2-99</span>
                    </div>
                    <button className="p-2 text-white hover:text-[#00AE9D] transition-colors">
                      <Copy size={16} />
                    </button>
                  </div>

                  <PrimaryButton loading={loading} onClick={handleSetupComplete}>
                    ATIVAR DISPOSITIVO <CheckCircle2 size={18} />
                  </PrimaryButton>
                </div>
              </div>
            )}

            {step === 'mfa' && (
              <div key="mfa" className="w-full space-y-10 animate-in fade-in slide-in-from-bottom-8 duration-1000">
                <div className="text-center space-y-6">
                  <div className="inline-flex p-6 bg-[#00AE9D]/10 rounded-lg text-[#00AE9D] animate-in zoom-in duration-700 border border-[#00AE9D]/20">
                    <Lock size={40} className="animate-pulse" />
                  </div>
                  <div className="space-y-2">
                    <h2 className="text-3xl font-black text-white">Verificação</h2>
                    <p className="text-white/80 text-sm max-w-[320px] mx-auto leading-relaxed font-medium">
                      Insira o código gerado no seu dispositivo.
                    </p>
                  </div>
                </div>

                <MfaCodeInput onComplete={handleMfaComplete} delay={300} />

                <div className="space-y-4">
                  <PrimaryButton loading={loading} onClick={() => handleMfaComplete()} delay={500}>
                    CONFIRMAR ACESSO <ShieldCheck size={18} />
                  </PrimaryButton>
                  
                  <div className="flex justify-center">
                    <button 
                      onClick={() => setStep('login')}
                      className="h-10 text-[10px] font-black text-white/60 hover:text-[#00AE9D] transition-colors uppercase tracking-[3px]"
                    >
                      Voltar ao início
                    </button>
                  </div>
                </div>
              </div>
            )}

            {step === 'success' && (
              <div key="success" className="w-full text-center space-y-10 animate-in fade-in zoom-in-95 duration-1000">
                <div className="relative inline-flex">
                  <div className="absolute inset-0 bg-[#00AE9D]/40 blur-[40px] rounded-lg animate-pulse" />
                  <div className="relative w-36 h-36 bg-[#00AE9D] rounded-lg flex items-center justify-center text-white shadow-2xl transform rotate-3">
                    <CheckCircle2 size={72} className="animate-in fade-in zoom-in-50 duration-500 delay-300 -rotate-3" />
                  </div>
                </div>
                <div className="space-y-3">
                  <h2 className="text-3xl font-black text-white">Acesso Autorizado</h2>
                  <p className="text-white/90 font-bold tracking-tight text-base">Iniciando ambiente de governança corporativa...</p>
                </div>
                <div className="pt-4 flex justify-center">
                   <RefreshCw size={24} className="text-[#00AE9D] animate-spin" />
                </div>
              </div>
            )}
          </div>

          {/* Rodapé da Sidebar */}
          <div className="relative z-10 animate-in fade-in duration-1000" style={{ animationDelay: '500ms' }}>
            <div className="h-px bg-white/20 dark:bg-white/10 w-full mb-8" />
            <div className="flex justify-between items-center text-[10px] font-black text-white uppercase tracking-[1.5px]">
              <div className="flex flex-col gap-1.5">
                <span>Sicoob Buritis</span>
                <span className="opacity-60 text-[9px] tracking-[0.5px]">RO • BRASIL</span>
              </div>
              <div className="flex flex-col items-end gap-1.5">
                <div className="flex items-center gap-2">
                  <span className="w-2 h-2 rounded-full bg-emerald-500 shadow-[0_0_12px_rgba(16,185,129,0.8)] animate-pulse" />
                  <span className="text-[#00AE9D] font-black tracking-widest">ONLINE</span>
                </div>
              </div>
            </div>
          </div>
        </aside>

        {/* ÁREA DIREITA */}
        <main className="hidden lg:block flex-1 relative h-full">
          <div className="absolute bottom-10 right-10 z-20 animate-in fade-in slide-in-from-right-8 duration-1000" style={{ animationDelay: '1000ms' }}>
              <div className="flex items-center gap-4 text-white/50 hover:text-white transition-all cursor-pointer group bg-black/40 backdrop-blur-xl px-5 py-3 rounded-lg border border-white/10 hover:border-white/20">
                  <span className="text-[10px] font-black tracking-[2px] uppercase">Política de Segurança</span>
                  <ChevronRight size={14} className="group-hover:translate-x-1.5 transition-transform" />
              </div>
          </div>
        </main>
      </div>

      <style>
        {`
        @import url('https://fonts.googleapis.com/css2?family=Inter:wght@400;500;700;900&display=swap');
        body { font-family: 'Inter', sans-serif; overflow: hidden; background-color: #000; }
        .animate-in { animation-fill-mode: both; }
        ::-webkit-scrollbar { width: 0px; }
        
        .dark input { 
          background-color: rgba(0, 0, 0, 0.6) !important; 
          border-color: rgba(255, 255, 255, 0.1) !important;
          color: white !important; 
          backdrop-filter: blur(5px);
        }
        .dark input:focus {
          background-color: rgba(0, 0, 0, 0.8) !important;
          border-color: #00AE9D !important;
        }
        .dark input::placeholder { 
          color: rgba(255, 255, 255, 0.7) !important; 
        }

        .backdrop-blur-[35px] { backdrop-filter: blur(35px); -webkit-backdrop-filter: blur(35px); }
        `}
      </style>
    </div>
  );
}