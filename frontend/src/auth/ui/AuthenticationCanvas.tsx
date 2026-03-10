import { useCallback, useEffect, useMemo, useRef, useState } from 'react'
import {
  ArrowLeft,
  ArrowRight,
  CheckCircle2,
  Copy,
  Lock,
  Mail,
  Moon,
  QrCode,
  RefreshCw,
  Shield,
  ShieldCheck,
  Smartphone,
  Sun,
  User,
} from 'lucide-react'
import type { LucideIcon } from 'lucide-react'
import { useNavigate } from '@tanstack/react-router'
import { useShallow } from 'zustand/react/shallow'
import loginBackground from '@/assets/login-bg.jpg'
import { authStore } from '@/auth/store'
import type { MfaSetupResponse } from '@/auth/types'
import { appConfig } from '@/lib/config'
import { pushNotification } from '@/shared/notifications/notificationStore'

type PrimaryButtonProps = {
  children: React.ReactNode
  onClick?: () => void
  loading?: boolean
  disabled?: boolean
  className?: string
  delay?: string
  type?: 'button' | 'submit'
}

function PrimaryButton({
  children,
  onClick,
  loading,
  disabled,
  className = '',
  delay = '0',
  type = 'button',
}: PrimaryButtonProps) {
  return (
    <div
      className="animate-in fade-in slide-in-from-bottom-4 duration-700 fill-mode-both"
      style={{ animationDelay: `${delay}ms` }}
    >
      <button
        type={type}
        onClick={onClick}
        disabled={loading || disabled}
        className={`w-full h-12 flex items-center justify-center gap-3 rounded-lg font-bold text-white shadow-md transition-colors
          ${loading || disabled ? 'bg-slate-500/80 cursor-not-allowed' : 'bg-[#00AE9D] hover:shadow-[#00AE9D]/30 hover:brightness-105'}
          ${className}`}
      >
        {loading ? <RefreshCw className="w-5 h-5 animate-spin" /> : children}
      </button>
    </div>
  )
}

type AuthInputProps = {
  label: string
  icon: LucideIcon
  type?: string
  placeholder: string
  value: string
  onChange: (value: string) => void
  onKeyDown?: (event: React.KeyboardEvent<HTMLInputElement>) => void
  delay?: string
}

function AuthInput({
  label,
  icon: Icon,
  type = 'text',
  placeholder,
  value,
  onChange,
  onKeyDown,
  delay = '0',
}: AuthInputProps) {
  return (
    <div
      className="space-y-1.5 group animate-in fade-in slide-in-from-bottom-4 fill-mode-both duration-700"
      style={{ animationDelay: `${delay}ms` }}
    >
      <div className="flex justify-between items-center px-1">
        <label className="text-[10px] font-black text-white uppercase tracking-[1.5px]">{label}</label>
      </div>
      <div className="relative">
        <div className="absolute left-4 top-1/2 -translate-y-1/2 text-white/60 group-focus-within:text-[#00AE9D] transition-colors">
          <Icon size={16} />
        </div>
        <input
          type={type}
          value={value}
          onChange={(event) => onChange(event.target.value)}
          onKeyDown={onKeyDown}
          placeholder={placeholder}
          className="w-full h-12 pl-11 pr-4 bg-white/30 dark:bg-black/40 border border-white/20 dark:border-white/10 rounded-lg outline-none transition-all
          focus:bg-white dark:focus:bg-slate-800 focus:border-[#00AE9D] text-slate-800 dark:text-white
          placeholder:text-slate-100 dark:placeholder:text-white shadow-sm font-semibold"
        />
      </div>
    </div>
  )
}

type MfaCodeInputProps = {
  value: string
  onChange: (value: string) => void
  onComplete: (code: string) => void
  delay?: string
}

function MfaCodeInput({ value, onChange, onComplete, delay = '0' }: MfaCodeInputProps) {
  const [code, setCode] = useState<string[]>(['', '', '', '', '', ''])
  const inputs = useRef<Array<HTMLInputElement | null>>([])

  useEffect(() => {
    const next = ['', '', '', '', '', '']
    const normalized = value.replace(/\D/g, '').slice(0, 6)
    for (let index = 0; index < normalized.length; index += 1) {
      next[index] = normalized[index]
    }
    setCode(next)
  }, [value])

  const handleChange = (index: number, rawValue: string) => {
    if (rawValue && Number.isNaN(Number(rawValue))) return

    const newCode = [...code]
    newCode[index] = rawValue.substring(rawValue.length - 1)
    setCode(newCode)

    if (rawValue && index < 5) {
      inputs.current[index + 1]?.focus()
    }

    const joined = newCode.join('')
    onChange(joined)
    if (newCode.every((digit) => digit !== '')) {
      onComplete(joined)
    }
  }

  const handleKeyDown = (index: number, event: React.KeyboardEvent<HTMLInputElement>) => {
    if (event.key === 'Backspace' && !code[index] && index > 0) {
      inputs.current[index - 1]?.focus()
    }
  }

  return (
    <div
      className="flex justify-between gap-2 animate-in fade-in duration-700 fill-mode-both"
      style={{ animationDelay: `${delay}ms` }}
    >
      {code.map((digit, index) => (
        <input
          key={index}
          ref={(element) => {
            inputs.current[index] = element
          }}
          type="text"
          maxLength={1}
          value={digit}
          onChange={(event) => handleChange(index, event.target.value)}
          onKeyDown={(event) => handleKeyDown(index, event)}
          className="w-full h-12 text-center text-xl font-black bg-white/30 dark:bg-black/50 border border-white/20 dark:border-white/10 focus:border-[#00AE9D] rounded-lg outline-none text-white transition-all focus:ring-4 focus:ring-[#00AE9D]/10"
          style={{ animationDelay: `${Number.parseInt(delay, 10) + index * 60}ms` }}
        />
      ))}
    </div>
  )
}

function SidebarIconGrid() {
  const icons = Array.from({ length: 48 })
  return (
    <div className="absolute inset-0 grid grid-cols-6 gap-8 p-10 pointer-events-none opacity-[0.1] dark:opacity-[0.2]">
      {icons.map((_, index) => (
        <div key={index} className="flex items-center justify-center text-white/40 dark:text-[#00AE9D]">
          {index % 3 === 0 ? <Shield size={28} /> : index % 3 === 1 ? <Lock size={28} /> : <ShieldCheck size={28} />}
        </div>
      ))}
    </div>
  )
}

function BackgroundHero() {
  return (
    <div className="fixed inset-0 z-0 bg-black">
      <img
        src={loginBackground}
        alt="Sicoob Corporate Background"
        className="w-full h-full object-cover brightness-[0.6] contrast-125 animate-in fade-in zoom-in-105 duration-[2000ms]"
      />
      <div className="absolute inset-0 bg-gradient-to-r from-black/20 via-transparent to-transparent dark:from-black/40" />
      <div className="absolute inset-0 bg-black/10 dark:bg-black/20" />
    </div>
  )
}

type CanvasStep = 'login' | 'forgot-password' | 'setup' | 'mfa' | 'success'
type MfaPurpose = 'loginChallenge' | 'setupConfirm' | null

export function AuthenticationCanvas() {
  const [theme, setTheme] = useState<'light' | 'dark'>('light')
  const [step, setStep] = useState<CanvasStep>('login')
  const [isQrExpanded, setIsQrExpanded] = useState(false)
  const [mousePos, setMousePos] = useState({ x: 0, y: 0 })
  const [backendOnline, setBackendOnline] = useState(false)
  const [backendProbePending, setBackendProbePending] = useState(true)
  const [mfaPurpose, setMfaPurpose] = useState<MfaPurpose>(null)
  const [mfaCode, setMfaCode] = useState('')
  const [mfaSetup, setMfaSetup] = useState<MfaSetupResponse | null>(null)
  const [recoveryCodes, setRecoveryCodes] = useState<string[]>([])
  const [resetFactor, setResetFactor] = useState<'totp' | 'recovery'>('totp')
  const [loginForm, setLoginForm] = useState({ username: '', password: '' })
  const [forgotForm, setForgotForm] = useState({
    username: '',
    newPassword: '',
    totpCode: '',
    recoveryCode: '',
  })

  const navigate = useNavigate()
  const containerRef = useRef<HTMLDivElement | null>(null)
  const successTimeout = useRef<number | null>(null)
  const backendProbeInterval = useRef<number | null>(null)

  const {
    login,
    beginMfaSetup,
    confirmMfa,
    resetPassword,
    challengeId,
    isLoading,
    error,
    clearError,
  } = authStore(
    useShallow((state) => ({
      login: state.login,
      beginMfaSetup: state.beginMfaSetup,
      confirmMfa: state.confirmMfa,
      resetPassword: state.resetPassword,
      challengeId: state.challengeId,
      isLoading: state.isLoading,
      error: state.error,
      clearError: state.clearError,
    })),
  )

  useEffect(
    () => () => {
      if (successTimeout.current) {
        window.clearTimeout(successTimeout.current)
      }
      if (backendProbeInterval.current) {
        window.clearInterval(backendProbeInterval.current)
      }
    },
    [],
  )

  useEffect(() => {
    if (!isQrExpanded) return

    const onKeyDown = (event: KeyboardEvent) => {
      if (event.key === 'Escape') {
        setIsQrExpanded(false)
      }
    }

    window.addEventListener('keydown', onKeyDown)
    return () => window.removeEventListener('keydown', onKeyDown)
  }, [isQrExpanded])

  const setupSecret = useMemo(() => extractSecretFromOtpauth(mfaSetup?.otpauth_url ?? ''), [mfaSetup?.otpauth_url])

  const notify = useCallback(
    (
      message: string,
      tone: 'info' | 'success' | 'warning' | 'error' = 'info',
      title?: string,
      durationMs?: number,
    ) => {
      pushNotification({
        message,
        tone,
        title,
        durationMs,
      })
    },
    [],
  )

  const probeBackendOnline = useCallback(async (showPending: boolean) => {
    if (showPending) {
      setBackendProbePending(true)
    }

    const controller = new AbortController()
    const timeoutId = window.setTimeout(() => controller.abort(), 2500)
    const baseUrl = appConfig.apiBaseUrl.replace(/\/+$/, '')

    try {
      const response = await fetch(`${baseUrl}/health`, {
        method: 'GET',
        cache: 'no-store',
        credentials: 'omit',
        signal: controller.signal,
      })
      setBackendOnline(response.ok)
    } catch {
      setBackendOnline(false)
    } finally {
      window.clearTimeout(timeoutId)
      if (showPending) {
        setBackendProbePending(false)
      }
    }
  }, [])

  useEffect(() => {
    void probeBackendOnline(true)
    backendProbeInterval.current = window.setInterval(() => {
      void probeBackendOnline(false)
    }, 10000)
    return () => {
      if (backendProbeInterval.current) {
        window.clearInterval(backendProbeInterval.current)
        backendProbeInterval.current = null
      }
    }
  }, [probeBackendOnline])

  useEffect(() => {
    if (!error) return
    notify(mapAuthErrorMessage(error), 'error', 'Falha de autenticação')
  }, [error, notify])

  const handleMouseMove = (event: React.MouseEvent<HTMLDivElement>) => {
    if (!containerRef.current) return
    const rect = containerRef.current.getBoundingClientRect()
    setMousePos({ x: event.clientX - rect.left, y: event.clientY - rect.top })
  }

  const transitionToSuccess = () => {
    setStep('success')
    successTimeout.current = window.setTimeout(() => {
      navigate({ to: '/app' })
    }, 1200)
  }

  const handleLogin = async () => {
    clearError()

    if (!backendOnline) {
      notify('Backend indisponível. Verifique o serviço e tente novamente.', 'warning', 'Offline')
      return
    }

    if (!loginForm.username || !loginForm.password) {
      notify('Preencha usuário e senha para continuar.', 'warning', 'Campos obrigatórios')
      return
    }

    try {
      const response = await login({
        username: loginForm.username,
        password: loginForm.password,
      })

      if ('mfa_required' in response) {
        setMfaPurpose('loginChallenge')
        setMfaCode('')
        setStep('mfa')
        return
      }

      if (response.user.mfa_enabled) {
        transitionToSuccess()
        return
      }

      const setup = await beginMfaSetup()
      setMfaSetup(setup)
      setRecoveryCodes([])
      setMfaCode('')
      setMfaPurpose('setupConfirm')
      setStep('setup')
    } catch {}
  }

  const handleForgotPassword = async () => {
    clearError()

    if (!backendOnline) {
      notify('Backend indisponível. Verifique o serviço e tente novamente.', 'warning', 'Offline')
      return
    }

    if (!forgotForm.username || !forgotForm.newPassword) {
      notify('Informe usuário e nova senha.', 'warning', 'Campos obrigatórios')
      return
    }

    if (resetFactor === 'totp' && !forgotForm.totpCode) {
      notify('Informe o código TOTP para redefinir a senha.', 'warning', 'Fator obrigatório')
      return
    }

    if (resetFactor === 'recovery' && !forgotForm.recoveryCode) {
      notify('Informe um recovery code para redefinir a senha.', 'warning', 'Fator obrigatório')
      return
    }

    try {
      const response =
        resetFactor === 'totp'
          ? await resetPassword({
              username: forgotForm.username,
              new_password: forgotForm.newPassword,
              totp_code: forgotForm.totpCode,
            })
          : await resetPassword({
              username: forgotForm.username,
              new_password: forgotForm.newPassword,
              recovery_code: forgotForm.recoveryCode,
            })

      setLoginForm((current) => ({ ...current, username: forgotForm.username }))
      notify(response.message || 'Senha redefinida com sucesso.', 'success', 'Senha atualizada')
      setStep('login')
    } catch {}
  }

  const handleSetupComplete = () => {
    clearError()
    setStep('mfa')
  }

  const handleMfaComplete = async () => {
    clearError()

    if (!backendOnline) {
      notify('Backend indisponível. Verifique o serviço e tente novamente.', 'warning', 'Offline')
      return
    }

    if (mfaCode.length !== 6) {
      notify('Digite os 6 dígitos do autenticador.', 'warning', 'Código incompleto')
      return
    }

    try {
      if (mfaPurpose === 'loginChallenge') {
        if (!challengeId) {
          notify('Desafio expirado. Faça login novamente.', 'warning', 'Sessão de MFA expirada')
          setStep('login')
          return
        }

        const response = await login({
          username: loginForm.username,
          password: loginForm.password,
          totp_code: mfaCode,
          challenge_id: challengeId,
        })

        if ('mfa_required' in response) {
          notify('Código inválido. Tente novamente.', 'warning', 'MFA inválido')
          return
        }

        transitionToSuccess()
        return
      }

      if (mfaPurpose === 'setupConfirm') {
        if (recoveryCodes.length > 0) {
          transitionToSuccess()
          return
        }

        const response = await confirmMfa({ totp_code: mfaCode })
        setRecoveryCodes(response.recovery_codes)
        notify('MFA confirmado. Copie os recovery codes antes de continuar.', 'success', 'Dispositivo vinculado', 5200)
      }
    } catch {}
  }

  const handleCopy = async (value: string, successMessage: string) => {
    if (!value) return
    try {
      await navigator.clipboard.writeText(value)
      notify(successMessage, 'success', 'Copiado')
    } catch {
      notify('Não foi possível copiar automaticamente neste navegador.', 'warning', 'Falha ao copiar')
    }
  }

  const backendStatusLabel = backendProbePending ? 'CHECANDO' : backendOnline ? 'ONLINE' : 'OFFLINE'
  const backendStatusDotClass = backendProbePending
    ? 'bg-amber-400 shadow-[0_0_12px_rgba(251,191,36,0.7)] animate-pulse'
    : backendOnline
      ? 'bg-emerald-500 shadow-[0_0_12px_rgba(16,185,129,0.8)] animate-pulse'
      : 'bg-rose-500 shadow-[0_0_12px_rgba(244,63,94,0.6)]'
  const backendStatusTextClass = backendOnline ? 'text-[#00AE9D]' : backendProbePending ? 'text-amber-300' : 'text-rose-300'

  return (
    <div
      ref={containerRef}
      onMouseMove={handleMouseMove}
      className={`${theme} relative flex min-h-screen font-sans selection:bg-[#00AE9D]/30 transition-colors duration-500 overflow-hidden`}
      style={{
        '--mouse-x': `${mousePos.x}px`,
        '--mouse-y': `${mousePos.y}px`,
      } as React.CSSProperties}
    >
      <BackgroundHero />

      <div
        className="fixed inset-0 pointer-events-none z-10 transition-opacity duration-300 opacity-100"
        style={{
          background: `radial-gradient(600px circle at var(--mouse-x) var(--mouse-y), rgba(0, 174, 157, 0.12), transparent 80%)`,
        }}
      />

      <div className="relative z-20 flex w-full min-h-screen">
        <aside
          className="relative w-full md:w-[540px] flex flex-col justify-between p-10 md:p-14
          bg-white/10 dark:bg-black/40 backdrop-blur-[35px] backdrop-saturate-[2.5]
          border-r border-white/20 dark:border-white/5
          shadow-[50px_0_100px_-20px_rgba(0,0,0,0.3)] transition-all duration-700"
        >
          <div
            className="absolute inset-0 pointer-events-none overflow-hidden"
            style={{
              maskImage: `radial-gradient(400px circle at var(--mouse-x) var(--mouse-y), black 0%, transparent 100%)`,
              WebkitMaskImage: `radial-gradient(400px circle at var(--mouse-x) var(--mouse-y), black 0%, transparent 100%)`,
            }}
          >
            <SidebarIconGrid />
          </div>

          <div className="flex items-center justify-between relative z-10 animate-in fade-in slide-in-from-top-6 duration-1000">
            <div className="flex flex-col group cursor-default">
              <span className="font-black text-2xl tracking-[-0.05em] text-white uppercase leading-none">SICOOB</span>
              <span className="text-[10px] font-black text-[#00AE9D] tracking-[0.5em] uppercase mt-1.5">Compliance Gate</span>
            </div>
            <button
              onClick={() => setTheme((current) => (current === 'light' ? 'dark' : 'light'))}
              className="w-10 h-10 flex items-center justify-center rounded-lg bg-white/40 dark:bg-white/5 text-white border border-white/20 dark:border-white/10 hover:text-[#00AE9D] transition-all shadow-sm active:scale-90"
            >
              {theme === 'light' ? <Moon size={18} /> : <Sun size={18} />}
            </button>
          </div>

          <div className="flex-1 flex items-center justify-center py-10 relative z-10">
            {step === 'login' && (
              <form
                key="login"
                className="w-full space-y-10 animate-in fade-in slide-in-from-bottom-8 duration-1000"
                onSubmit={(event) => {
                  event.preventDefault()
                  void handleLogin()
                }}
              >
                <div className="flex items-center gap-5">
                  <div className="w-1.5 h-12 bg-[#00AE9D] rounded-full shadow-[0_0_15px_rgba(0,174,157,0.5)]" />
                  <h1 className="text-3xl font-black text-white tracking-tight">Bem-vindo de volta.</h1>
                </div>

                <div className="space-y-4">
                  <AuthInput
                    label="Identidade Corporativa"
                    icon={User}
                    placeholder="nome.sobrenome"
                    value={loginForm.username}
                    onChange={(value) => setLoginForm((current) => ({ ...current, username: value }))}
                    delay="200"
                  />
                  <AuthInput
                    label="Senha de Rede"
                    icon={Lock}
                    type="password"
                    placeholder="••••••••"
                    value={loginForm.password}
                    onChange={(value) => setLoginForm((current) => ({ ...current, password: value }))}
                    delay="400"
                  />
                </div>

                <PrimaryButton type="submit" loading={isLoading} disabled={!backendOnline || backendProbePending} delay="600">
                  EFETUAR LOGIN <ArrowRight size={18} />
                </PrimaryButton>

                <div
                  className="flex items-center justify-center gap-4 pt-2 text-[9px] font-black text-white/70 dark:text-white/60 uppercase tracking-widest animate-in fade-in duration-1000"
                  style={{ animationDelay: '800ms' }}
                >
                  <button type="button" onClick={() => setStep('forgot-password')} className="hover:text-[#00AE9D] transition-colors">
                    Esqueci minha senha
                  </button>
                  <span className="w-1 h-1 rounded-full bg-white/30 dark:bg-white/20" />
                  <button type="button" className="hover:text-[#00AE9D] transition-colors">
                    Suporte TI
                  </button>
                </div>
              </form>
            )}

            {step === 'forgot-password' && (
              <form
                key="forgot-password"
                className="w-full space-y-10 animate-in fade-in slide-in-from-bottom-8 duration-1000"
                onSubmit={(event) => {
                  event.preventDefault()
                  void handleForgotPassword()
                }}
              >
                <div className="flex items-center gap-5">
                  <button
                    type="button"
                    onClick={() => setStep('login')}
                    className="p-2 -ml-2 text-white hover:text-[#00AE9D] transition-colors"
                  >
                    <ArrowLeft size={24} />
                  </button>
                  <h1 className="text-3xl font-black text-white tracking-tight">Recuperar Acesso</h1>
                </div>

                <div className="space-y-4">
                  <p className="text-xs text-white/80 font-medium px-1 leading-relaxed">
                    Insira seu e-mail corporativo ou CPF. Enviaremos as instruções de redefinição para o seu dispositivo vinculado.
                  </p>
                  <AuthInput
                    label="Identificação"
                    icon={Mail}
                    placeholder="nome.sobrenome"
                    value={forgotForm.username}
                    onChange={(value) => setForgotForm((current) => ({ ...current, username: value }))}
                    delay="200"
                  />
                  <AuthInput
                    label="Nova Senha"
                    icon={Lock}
                    type="password"
                    placeholder="••••••••"
                    value={forgotForm.newPassword}
                    onChange={(value) => setForgotForm((current) => ({ ...current, newPassword: value }))}
                    delay="260"
                  />

                  <div className="flex items-center justify-center gap-4 pt-1 text-[9px] font-black text-white/70 uppercase tracking-widest">
                    <button
                      type="button"
                      onClick={() => setResetFactor('totp')}
                      className={`${resetFactor === 'totp' ? 'text-[#00AE9D]' : 'hover:text-[#00AE9D]'} transition-colors`}
                    >
                      Usar TOTP
                    </button>
                    <span className="w-1 h-1 rounded-full bg-white/30" />
                    <button
                      type="button"
                      onClick={() => setResetFactor('recovery')}
                      className={`${resetFactor === 'recovery' ? 'text-[#00AE9D]' : 'hover:text-[#00AE9D]'} transition-colors`}
                    >
                      Recovery Code
                    </button>
                  </div>

                  {resetFactor === 'totp' ? (
                    <AuthInput
                      label="Código TOTP"
                      icon={ShieldCheck}
                      placeholder="123456"
                      value={forgotForm.totpCode}
                      onChange={(value) => setForgotForm((current) => ({ ...current, totpCode: value.replace(/\D/g, '').slice(0, 12) }))}
                      delay="320"
                    />
                  ) : (
                    <AuthInput
                      label="Recovery Code"
                      icon={ShieldCheck}
                      placeholder="ABCD-1234"
                      value={forgotForm.recoveryCode}
                      onChange={(value) => setForgotForm((current) => ({ ...current, recoveryCode: value.trim() }))}
                      delay="320"
                    />
                  )}
                </div>

                <PrimaryButton
                  type="submit"
                  loading={isLoading}
                  disabled={!backendOnline || backendProbePending}
                  delay="400"
                >
                  SOLICITAR NOVA SENHA <ArrowRight size={18} />
                </PrimaryButton>

                <p className="text-center text-[9px] font-black text-white/50 uppercase tracking-widest">
                  Protegido por Compliance Gate
                </p>
              </form>
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
                  {mfaSetup ? (
                    <img
                      src={`data:image/png;base64,${mfaSetup.qr_code_base64_png}`}
                      alt="QR Code MFA"
                      className="relative z-10 w-32 h-32 rounded-md cursor-zoom-in transition-transform hover:scale-105"
                      onClick={() => setIsQrExpanded(true)}
                    />
                  ) : (
                    <QrCode size={100} className="text-[#00AE9D] relative z-10" />
                  )}
                  <div className="absolute -top-2 -right-2 bg-[#00AE9D] text-white p-1 rounded-md shadow-lg">
                    <Smartphone size={14} />
                  </div>
                </div>

                <div className="space-y-4">
                  <div className="p-3 bg-white/50 dark:bg-white/5 border border-white/20 rounded-lg flex items-center justify-between group">
                    <div className="flex flex-col">
                      <span className="text-[8px] font-black text-white/40 uppercase tracking-widest">Chave Secreta</span>
                      <span className="text-xs font-mono font-bold text-white tracking-widest uppercase">
                        {setupSecret || 'SICOOB-G8-F2-99'}
                      </span>
                    </div>
                    <button
                      onClick={() => handleCopy(mfaSetup?.otpauth_url ?? '', 'Link otpauth copiado.')}
                      className="p-2 text-white hover:text-[#00AE9D] transition-colors"
                    >
                      <Copy size={16} />
                    </button>
                  </div>

                  <PrimaryButton loading={isLoading} onClick={handleSetupComplete} disabled={!mfaSetup}>
                    ATIVAR DISPOSITIVO <CheckCircle2 size={18} />
                  </PrimaryButton>
                </div>
              </div>
            )}

            {step === 'mfa' && (
              <form
                key="mfa"
                className="w-full space-y-10 animate-in fade-in slide-in-from-bottom-8 duration-1000"
                onSubmit={(event) => {
                  event.preventDefault()
                  void handleMfaComplete()
                }}
              >
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

                <MfaCodeInput value={mfaCode} onChange={setMfaCode} onComplete={setMfaCode} delay="300" />

                {recoveryCodes.length > 0 && (
                  <div className="p-3 bg-white/50 dark:bg-white/5 border border-white/20 rounded-lg flex items-center justify-between gap-4 group">
                    <div className="flex flex-col gap-1 overflow-hidden">
                      <span className="text-[8px] font-black text-white/40 uppercase tracking-widest">Recovery Codes</span>
                      <span className="text-[10px] font-mono font-bold text-white tracking-widest uppercase truncate">
                        {recoveryCodes.join(' • ')}
                      </span>
                    </div>
                    <button
                      onClick={() => handleCopy(recoveryCodes.join('\n'), 'Recovery codes copiados.')}
                      className="p-2 text-white hover:text-[#00AE9D] transition-colors"
                    >
                      <Copy size={16} />
                    </button>
                  </div>
                )}

                <div className="space-y-4">
                  <PrimaryButton type="submit" loading={isLoading} disabled={!backendOnline || backendProbePending} delay="500">
                    {recoveryCodes.length > 0 ? 'CONTINUAR' : 'CONFIRMAR ACESSO'} <ShieldCheck size={18} />
                  </PrimaryButton>

                  <div className="flex justify-center">
                    <button
                      type="button"
                      onClick={() => {
                        setStep('login')
                        setMfaCode('')
                        setRecoveryCodes([])
                        setMfaPurpose(null)
                      }}
                      className="h-10 text-[10px] font-black text-white/60 hover:text-[#00AE9D] transition-colors uppercase tracking-[3px]"
                    >
                      Voltar ao início
                    </button>
                  </div>
                </div>
              </form>
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

          <div className="relative z-10 animate-in fade-in duration-1000" style={{ animationDelay: '500ms' }}>
            {step === 'setup' && mfaSetup?.instructions && (
              <div className="mb-4 rounded-lg border border-[#00AE9D]/30 bg-[#00AE9D]/10 px-3 py-2 text-[10px] font-bold tracking-[0.8px] text-white/90">
                {mfaSetup.instructions}
              </div>
            )}
            <div className="flex justify-end items-center text-[10px] font-black uppercase tracking-[1.5px]">
              <div className="flex items-center gap-2">
                <span className={`w-2 h-2 rounded-full ${backendStatusDotClass}`} />
                <span className={`${backendStatusTextClass} font-black tracking-widest`}>{backendStatusLabel}</span>
              </div>
            </div>
          </div>
        </aside>

        <main className="hidden lg:block flex-1 relative h-full" />
      </div>

      {isQrExpanded && mfaSetup && (
        <div
          className="fixed inset-0 z-[120] bg-black/80 backdrop-blur-sm flex items-center justify-center p-4 md:p-8 animate-in fade-in duration-200"
          onClick={() => setIsQrExpanded(false)}
        >
          <button
            type="button"
            onClick={() => setIsQrExpanded(false)}
            className="absolute top-5 right-5 md:top-8 md:right-8 bg-black/60 hover:bg-black/80 border border-white/20 text-white text-xs font-black uppercase tracking-[1.5px] px-3 py-2 rounded-lg"
          >
            Fechar
          </button>
          <img
            src={`data:image/png;base64,${mfaSetup.qr_code_base64_png}`}
            alt="QR Code MFA em tela cheia"
            className="w-[min(92vw,980px)] h-auto max-h-[92vh] rounded-xl border-4 border-white/90 bg-white p-3 shadow-[0_30px_120px_rgba(0,0,0,0.7)]"
            onClick={(event) => event.stopPropagation()}
          />
        </div>
      )}

      <style>
        {`
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
  )
}

function extractSecretFromOtpauth(otpauthUrl: string): string {
  if (!otpauthUrl) return ''
  try {
    const parsed = new URL(otpauthUrl)
    return (parsed.searchParams.get('secret') ?? '').replace(/(.{4})/g, '$1-').replace(/-$/, '')
  } catch {
    return ''
  }
}

function mapAuthErrorMessage(message: string): string {
  const normalized = message.toLowerCase().trim()

  if (normalized.includes('temporarily locked')) {
    return 'Conta temporariamente bloqueada por tentativas inválidas. Aguarde alguns minutos antes de tentar novamente.'
  }

  if (normalized.includes('invalid credentials')) {
    return 'Usuário ou senha inválidos.'
  }

  if (normalized.includes('network error')) {
    return 'Falha de rede ao alcançar o backend.'
  }

  return message.replace(/\s+/g, ' ').trim().slice(0, 220)
}
