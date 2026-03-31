import { Slot } from "@radix-ui/react-slot";
import { cva, type VariantProps } from "class-variance-authority";
import * as React from "react";

import { cn } from "@/lib/utils/cn";

const buttonVariants = cva(
  "inline-flex items-center justify-center gap-2 whitespace-nowrap rounded-md border text-body-strong transition-colors focus-visible:outline-none disabled:pointer-events-none disabled:opacity-45",
  {
    variants: {
      variant: {
        default:
          "border-border-subtle bg-surface text-foreground hover:border-border hover:bg-surface-alt",
        accent:
          "border-transparent bg-accent text-[var(--color-text-inverse)] hover:bg-accent-hover",
        ghost: "border-transparent bg-transparent text-foreground-secondary hover:bg-surface-alt hover:text-foreground",
        subtle:
          "border-border-subtle bg-surface-alt text-foreground-secondary hover:border-border hover:text-foreground",
      },
      size: {
        sm: "h-8 px-3 text-caption",
        md: "h-9 px-3.5 text-body",
        lg: "h-10 px-4 text-body-strong",
        icon: "h-9 w-9",
      },
    },
    defaultVariants: {
      variant: "default",
      size: "md",
    },
  },
);

export interface ButtonProps
  extends React.ButtonHTMLAttributes<HTMLButtonElement>,
    VariantProps<typeof buttonVariants> {
  asChild?: boolean;
}

const Button = React.forwardRef<HTMLButtonElement, ButtonProps>(
  ({ className, variant, size, asChild = false, ...props }, ref) => {
    const Comp = asChild ? Slot : "button";

    return <Comp className={cn(buttonVariants({ variant, size, className }))} ref={ref} {...props} />;
  },
);

Button.displayName = "Button";

export { Button, buttonVariants };
