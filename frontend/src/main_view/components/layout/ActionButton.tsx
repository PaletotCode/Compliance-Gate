import type { ReactNode } from 'react'

type ButtonVariant = 'primary' | 'secondary' | 'ghost' | 'danger'
type ButtonSize = 'sm' | 'md' | 'lg'

type ActionButtonProps = {
  children: ReactNode
  onClick?: () => void
  variant?: ButtonVariant
  size?: ButtonSize
  className?: string
  disabled?: boolean
  type?: 'button' | 'submit'
}

export function ActionButton({
  children,
  onClick,
  variant = 'primary',
  size = 'md',
  className = '',
  disabled = false,
  type = 'button',
}: ActionButtonProps) {
  const baseStyle =
    'font-bold flex items-center justify-center transition-all active:scale-[0.98] disabled:opacity-50 disabled:pointer-events-none tracking-wide rounded-lg outline-none focus-visible:ring-2 focus-visible:ring-offset-2 focus-visible:ring-offset-black focus-visible:ring-[#00AE9D] whitespace-nowrap shrink-0'

  const variants: Record<ButtonVariant, string> = {
    primary:
      'bg-[#00AE9D] text-white hover:bg-[#00AE9D]/90 hover:shadow-[0_0_15px_rgba(0,174,157,0.4)] border border-transparent',
    secondary: 'bg-white/10 backdrop-blur-md text-white hover:bg-white/20 border border-white/10 shadow-sm',
    ghost: 'text-white/60 hover:text-white hover:bg-white/5 border border-transparent',
    danger:
      'bg-red-500 text-white hover:bg-red-600 hover:shadow-[0_0_15px_rgba(239,68,68,0.4)] border border-transparent',
  }

  const sizes: Record<ButtonSize, string> = {
    sm: 'h-9 px-4 text-[11px] gap-2',
    md: 'h-10 px-4 text-xs gap-2',
    lg: 'h-12 px-6 text-sm gap-3',
  }

  return (
    <button
      type={type}
      onClick={onClick}
      disabled={disabled}
      className={`${baseStyle} ${variants[variant]} ${sizes[size]} ${className}`}
    >
      {children}
    </button>
  )
}
